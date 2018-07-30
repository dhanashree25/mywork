import com.diceplatform.brain.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

case class ExchangeRateConfig(path: String = "", dryRun: Boolean = false, separator: String = ",", header:Boolean = true, dateFormat:String = "dd MMM yyyy")

object ExchangeRateCSV extends Main{
  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[ExchangeRateConfig]("scopt") {
      head(
        """Extract-Transform-Load (ETL) task for populating exchange rate table by a CSV
          |
          |The file should follow the European Central Bank website
        """.stripMargin)

      opt[String]('p', "path")
        .action((x, c) => c.copy(path = x) )
        .required()
        .text("path to files, local or remote")
        .required()

      opt[Boolean]('d', "dry-run")
        .optional()
        .action((x, c) => c.copy(dryRun = x) )
        .text("dry run, default is false")

      opt[String]('s', "separator")
        .optional()
        .action((x, c) => c.copy(separator = x) )
        .text("the separator between columns, default is ,")

      opt[Boolean]('h', "header")
        .optional()
        .action((x, c) => c.copy(header = x) )
        .text("whether to use the header (first line) as column names, default is true")

      opt[String]('f', "date-format")
        .optional()
        .action((x, c) => c.copy(dateFormat = x) )
        .text("The date format to use, in Java date format. This can very between daily (dd MMM yyyy) vs all time (yyyy-MM-dd). default is dd MMM yyyy")
    }

    var cli: ExchangeRateConfig = ExchangeRateConfig()
    parser.parse(args, cli) match {
      case Some(c) => cli = c
      case None => System.exit(1)
    }


    val csv = spark.read
      .option("header", value=cli.header)
      .option("sep", value=cli.separator)
      .option("inferSchema", value=true)
      .csv(cli.path)

    val columns = csv.dtypes.map(column => column._1)

    val currencyColumns = columns.filter(column => column != "Date")

    val exchangeRate = csv
      .drop(columns.filter(_.matches("^_c[0-9]+$")).map(_.replace("_", "")): _*) // Drop unnamed columns (extra , in the header)
      .drop(columns.filter(_.matches("^ +$")): _*) // Drop columns which are just whitespace
      .select(
        unix_timestamp(col("Date"), cli.dateFormat)
          .cast(TimestampType) // convert unix timestamp (bigint) to timestamp first
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
              when(col(column) === "N/A", value=null)
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

    if (!cli.dryRun) {
      exchangeRate
        .write
        .redshift(spark)
        .option("dbtable", "exchange_rate")
        .mode(SaveMode.Append)
        .save()
    } else {
      println("columns", currencyColumns.mkString(","))
      exchangeRate.show(1000)
    }
  }
}
