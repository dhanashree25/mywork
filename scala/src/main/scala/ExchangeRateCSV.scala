import java.net.URL

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.nio.file.Files

import com.diceplatform.brain.implicits._

case class ExchangeRateConfig(
  url: String = "",
  dryRun: Boolean = false,
  separator: String = ",",
  header: Boolean = true,
  date: String = "",
  dateFormat: String = "dd MMM yyyy",
  dbTable: String = "exchange_rates",
  show: Int = 20
)

object ExchangeRateCSV extends Main {
  val parser: scopt.OptionParser[ExchangeRateConfig] = new scopt.OptionParser[ExchangeRateConfig]("scopt") {
    head(
      """Extract-Transform-Load (ETL) task for populating exchange rate table by a CSV
        |
        |The file should follow the European Central Bank website
      """.stripMargin)

    opt[String]("url")
      .action((x, c) => c.copy(url = x) )
      .required()
      .text("url to zip")

    opt[Boolean]("dry-run")
      .optional()
      .action((x, c) => c.copy(dryRun = x) )
      .text("dry run, default is false")

    opt[String]("separator")
      .optional()
      .action((x, c) => c.copy(separator = x) )
      .text("the separator between columns, default is ,")

    opt[Boolean]("header")
      .optional()
      .action((x, c) => c.copy(header = x) )
      .text("whether to use the header (first line) as column names, default is true")

    opt[String]("date")
      .action((x, c) => c.copy(date = x) )
      .required()
      .text("The date of exchange rates to process, this is usually only used on daily exchange rates CSV")

    opt[String]("date-format")
      .optional()
      .action((x, c) => c.copy(dateFormat = x) )
      .text("The date format to use, in Java date format. This can very between daily (dd MMM yyyy) vs all time (yyyy-MM-dd). default is dd MMM yyyy")

    opt[String]("dbtable")
      .optional()
      .action((x, c) => c.copy(dbTable = x) )
      .text("The database table to write the results to")

    opt[Int]("show")
      .optional()
      .action((x, c) => c.copy(show = x) )
      .text("The number of records to show during dry run")
  }

  val RANDOM_STRING_LENGTH = 5

  def main(args: Array[String]): Unit = {
    var cli: ExchangeRateConfig = ExchangeRateConfig()
    parser.parse(args, cli) match {
      case Some(c) => cli = c
      case None => System.exit(1)
    }

    val generator = new scala.util.Random().alphanumeric
    val prefix = generator.take(RANDOM_STRING_LENGTH).mkString("")

    val zipURL = new URL(cli.url)
    val zipPath = Files.createTempFile(prefix, ".zip")
    val zipDir = Files.createTempDirectory(prefix)

    DownloadUtil.download(zipURL, zipPath)
    val files = ZipUtil.unzip(zipPath, zipDir)

    if (files.length > 1) {
      throw new Exception("More than one file extracted")
    }

    println("Downloaded ZIP to", zipPath.toString)
    println("Unzipped CSV to", zipDir.toString)

    val csv = spark.read
      .option("header", value=cli.header)
      .option("sep", value=cli.separator)
      .option("inferSchema", value=true)
      .csv(files.head.toString)

    var exchangeRate = selectExchangeRates(csv, cli.dateFormat)

    if (cli.date != "") {
      exchangeRate = exchangeRate.where(
        col("date") === unix_timestamp(lit(cli.date), "yyyy/MM/dd").cast(TimestampType).cast(DateType)
      )

      if (exchangeRate.count() == 0) {
        throw new Exception(s"Date '${cli.date}' not found in exchange rates")
      }
    }

    if (!cli.dryRun) {
      exchangeRate
        .write
        .redshift(spark)
        .option("dbtable", cli.dbTable)
        .mode(SaveMode.Append)
        .save()
    } else {
      exchangeRate.show(cli.show)
    }
  }


  def selectExchangeRates(df: DataFrame, dateFormat: String): DataFrame = {
    val columns = df.dtypes.map(column => column._1)

    val currencyColumns = columns.filter(column => column != "Date").filter(!_.matches("^ +$"))
    val dropColumns = columns
      .filter(column => column.matches("^ +$") || column.matches("^_c[0-9]+$"))
      .map(_.replace("_", ""))

    df
      .drop(dropColumns: _*)
      .select(
        // Convert date into date type
        unix_timestamp(col("Date"), dateFormat)
          .cast(TimestampType)
          .cast(DateType)
          .alias("date"),
        // Explode it into new rows
        explode(
          array(
            // Create a new column which is an array of each currency/rate pair
            currencyColumns.map(column => struct(
              lit(column.trim)
                .cast(StringType)
                .alias("currency"),
              when(col(column) === "N/A", value = null)
                .otherwise(col(column))
                .cast(DecimalType(12, 4))
                .alias("rate")
            )): _*
          )
        ).alias("values")
      )
      .select(
        col("date"),
        col("values.currency"),
        col("values.rate")
      )
      .na.drop() // Drop values with null (N/A) this will be historical currencies
  }
}
