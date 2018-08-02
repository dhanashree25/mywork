import com.diceplatform.brain.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, IntegerType}

case class RollupConfig(
  path: String = "",
  dryRun: Boolean = false,
  from: String = "",
  to: String = "",
  window: String = "hour",
  show: Int = 20
)
object RevenueRollup extends Main{
  val parser: scopt.OptionParser[RollupConfig] = new scopt.OptionParser[RollupConfig]("scopt") {
    head(
      """Extract-Transform-Load (ETL) task for revenue_* roll-up table
      """.stripMargin
    )

    opt[Boolean]('d', "dry-run")
      .action((x, c) => c.copy(dryRun = x) )
      .optional()
      .text("dry run")

    opt[String]('w', "window")
      .required()
      .action((x, c) => c.copy(window = x) )
      .text("aggregate the records by a window, valid values are hour, day, week, month")

    opt[String]('f', "from")
      .optional()
      .action((x, c) => c.copy(from = x) )
      .text("the date range to select from (inclusive). Formatted at yyyy-MM-dd HH:mm:ss")

    opt[String]('t', "to")
      .optional()
      .action((x, c) => c.copy(to = x) )
      .text("the date range to select to (exclusive). Formatted at yyyy-MM-dd HH:mm:ss")

    opt[Int]('l', "show")
      .optional()
      .action((x, c) => c.copy(show = x) )
      .text("The number of records to show during dry run")
  }

  def selectPayments(df: DataFrame, from: String = "", to: String = ""): DataFrame = {
    var payments = df

    if (from != "") {
      payments = payments.where(col("ts") >= from)

      if (to != "") {
        payments = payments.where(col("ts") < to)
      }
    }

    payments
  }


  def main(args: Array[String]): Unit = {
    var cli: RollupConfig = RollupConfig()
    parser.parse(args, cli) match {
      case Some(c) => cli = c
      case None => System.exit(1)
    }

    val read_dbtable = "payment"

    val write_dbtable = "revenue_" + (cli.window.replaceAll("^[0-9]\\s+", "") match {
      case "day" => "daily"
      case period => period + "ly"
    })

    val exchangeRates = spark
      .read
      .redshift(spark)
      .option("dbtable", "exchange_rate")
      .load()

    // Get all payments within range
    var payments = spark
      .read
      .redshift(spark)
      .option("dbtable", "payment")
      .load()

    payments = selectPayments(payments, cli.from, cli.to)

    val revenueRollup = rollupRevenue(payments, cli.window)

    val revenueRollupInUSD = withColumnInCurrency(revenueRollup, exchangeRates)

    val dateTruncateFormat = cli.window match {
        case "month" => "MM"
        case "week" => "WEEK"
        case "day" => "DD"
        case "hour" => "HOUR"
    }

    val revenue = revenueRollupInUSD
      .withColumnRenamed("amount_with_tax", "amount")
      .withColumnRenamed("amount_with_tax_usd", "amount_usd")
      .withColumn("ts", date_trunc(dateTruncateFormat, col("ts")))

    if (!cli.dryRun) {
      revenue
        .write
        .redshift(spark)
        .option("dbtable", write_dbtable)
        .mode(SaveMode.Append)
        .save()
    } else {
      revenue.show(cli.show)
    }
 }

  /**
    * Adds a new column (amount_<currency>) which is in the requested currency
    *
    * It expects the data frame to have the column "amount_with_tax", "currency", and "ts"
    * It expects the exchange rate data frame to have the columns "currency", "rate" and "date"
    */
  def withColumnInCurrency(df: DataFrame,
                           exchangeRatesDf: DataFrame,
                           column:String = "amount_with_tax",
                           currency:String = "USD"
                  ): DataFrame = {
    val exchangeRateColumns = exchangeRatesDf.dtypes.map(dtype => dtype._1)

    // alias otherwise when dropping it gets confused
    val dfExtra = df
      .withColumn("ts_date", col("ts").cast(DateType))
      .withColumn("ts_weekday", date_format(col("ts_date"), "E"))
      .withColumnRenamed("currency", "source_currency")

    val amountInEUR = dfExtra
      .where(dfExtra.col("source_currency") =!= currency) // Exclude currency as already in the correct currency
      .join(
        right = exchangeRatesDf,
        joinExprs = exchangeRatesDf.col("currency") === dfExtra.col("source_currency") and closestDay(dfExtra, exchangeRatesDf),
        joinType = "left_outer"
      )
      .withColumn(s"${column}_eur", dfExtra.col(column) / exchangeRatesDf.col("rate"))
      .drop(exchangeRateColumns: _*)


    // Check for nulls as we do an left outer join, this is to prevent dropping records
    val nullEURCount = amountInEUR.where(col(s"${column}_eur").isNull).count()
    if (nullEURCount > 0) {
      throw new Exception("Rows exist with null EUR amount, this would be because the exchange rate doesn't exist")
    }

    // Join with USD exchange rates, so we can get EUR -> USD
    val dfInCurrency = amountInEUR
      .join(
        right=exchangeRatesDf,
        joinExprs=exchangeRatesDf.col("currency") === currency.toUpperCase() and closestDay(amountInEUR, exchangeRatesDf),
        joinType="left_outer"
      )
      .withColumn(s"${column}_" + currency.toLowerCase(), round(amountInEUR.col(s"${column}_eur") * exchangeRatesDf.col("rate")).cast(IntegerType))
      .drop(exchangeRateColumns: _*)
      .drop(s"${column}_eur")

    // Check for nulls as we do an left outer join, this is to prevent dropping records
    val nullCurrencyCount = dfInCurrency.where(col(s"${column}_usd").isNull).count()
    if (nullCurrencyCount > 0) {
      throw new Exception(s"Rows exist with null ${currency.toUpperCase} amount, this would be because the exchange rate doesn't exist")
    }

    // Add back rows already in the currency
    val dfAlreadyInCurrency = df
      .where(df.col("currency") === currency)
      .withColumn(s"${column}_usd", col(column))

    dfInCurrency
      .drop("ts_date")
      .drop("ts_weekday")
      .withColumnRenamed("source_currency", "currency")
      .union(dfAlreadyInCurrency)
  }

  /**
    * Calculate the aggregate of the revenue of a window duration by realm, payment provider and currency
    */
  def rollupRevenue(df: DataFrame, windowDuration: String): DataFrame = {
    df
      .na.fill(0, Seq("amount_with_tax"))
      .where(col("amount_with_tax") =!= 0.0)
      .cube(
        col("realm_id"),
        col("payment_provider"),
        col("currency"),
        window(col("ts"), "1 " + windowDuration).getItem("start").alias("ts")
      )
      .agg(
        sum(col("amount_with_tax")).alias("amount_with_tax")
      )
      .na.drop() // Drop cube by partial dimensions
  }


  /**
    * A condition which selects the closest weekday
    *
    * If the day is Sunday, it will select Friday
    * If the day is Saturday, it will select Friday
    * Otherwise, select the current day
    */
  private def closestDay(df1: DataFrame, df2: DataFrame): Column = {
    when(
      df1.col("ts_weekday") === "Sun",
      date_sub(df1.col("ts_date"), 2) === df2.col("date")
    ).otherwise(
      when(
        df1.col("ts_weekday") === "Sat",
        date_sub(df1.col("ts_date"), 1) === df2.col("date")
      ).otherwise(
        df1.col("ts_date") === df2.col("date")
      )
    )
  }
}
