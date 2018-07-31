import com.diceplatform.brain.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, DecimalType, IntegerType}

case class RollupConfig(path: String = "", dryRun: Boolean = false, from: String = "", to: String = "", window: String = "hour")
object RevenueRollup extends Main{
  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[RollupConfig]("scopt") {
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
    }

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

    val exchange_rates = spark
      .read
      .redshift(spark)
      .option("dbtable", "exchange_rate")
      .load()
      .as("er")

    val exchange_rate_columns = exchange_rates.dtypes.map(dtype => dtype._1)

    // Get all payments within range
    var payments = spark
      .read
      .redshift(spark)
      .option("dbtable", "payment")
      .load()

    if (cli.from != "") {
      payments = payments.where(col("ts") >= cli.from)

      if (cli.to != "") {
        payments = payments.where(col("ts") < cli.to)
      }
    }



    // Calculate a cube of all the dimensions we're interested in (realm_id, payment_provider, currency)
    // Aggregate by the amount
    val revenue_rollup = payments
      .na.fill(0, Seq("amount_with_tax"))
      .where(col("amount_with_tax") =!= 0.0)
      .cube(
        col("realm_id"),
        col("payment_provider"),
        col("currency"),
        window(col("ts"), "1 " + cli.window).getItem("start").alias("ts")
      )
      .agg(
        sum(col("amount_with_tax")).alias("amount")
      )
      .na.drop() // Drop cube by partial dimensions
      .as("r")
      .withColumnRenamed("currency", "source_currency")

    val revenue_rollup_columns = revenue_rollup.dtypes.map(dtype => col(dtype._1))

    // Add ts_date/ts_weekday to optimize joins, in theory Spark should only calculate it once.
    val revenue_with_dates = revenue_rollup
        .withColumn("ts_date", col("ts").cast(DateType))
        .withColumn("ts_weekday", date_format(col("ts_date"), "E"))

    // Join rollup with EUR exchange rates, as we only have A <-> EUR
    val revenue_with_eur = revenue_with_dates
      .where(revenue_with_dates.col("source_currency") =!= "USD") // Exclude USD as already in the correct currency
      .join(
        right=exchange_rates,
        joinExprs=exchange_rates.col("currency") === revenue_with_dates.col("source_currency") and whenWeekday(revenue_with_dates, exchange_rates),
        joinType="left_outer"
      )
      .withColumn("amount_eur", (revenue_with_dates.col("amount") / col("rate")).cast(IntegerType))
      .drop(exchange_rate_columns: _*)

    // Check for nulls as we do an left outer join, this is to prevent dropping records
    val null_with_eur = revenue_with_eur.where(col("amount_eur").isNull).count()
    if (null_with_eur > 0) {
      throw new Exception("Rows exist with null EUR amount, this would be because the exchange rate doesn't exist")
    }

    // Join with USD exchange rates, so we can get EUR -> USD
    val revenue_with_usd = revenue_with_eur
      .join(
        right=exchange_rates,
        joinExprs=exchange_rates.col("currency") === "USD" and whenWeekday(revenue_with_eur, exchange_rates),
        joinType="left_outer"
      )
      .withColumn("amount_usd", (revenue_with_eur.col("amount_eur") * col("rate")).cast(IntegerType))
      .drop(exchange_rate_columns: _*)

    // Check for nulls as we do an left outer join, this is to prevent dropping records
    val null_with_usd = revenue_with_usd.where(col("amount_usd").isNull).count()
    if (null_with_usd > 0) {
      throw new Exception("Rows exist with null USD amount, this would be because the exchange rate doesn't exist")
    }

    // Only select the original columns + amount_usd to save
    val eur_revenue = revenue_with_usd
      .select(
        revenue_rollup_columns ++ Array[Column](revenue_with_usd.col("amount_usd")): _*
      )

    val usd_revenue = revenue_rollup
      .where(revenue_with_dates.col("source_currency") === "USD")
      .withColumn("amount_usd", col("amount"))

    val revenue = eur_revenue.union(usd_revenue)
      .select(
        col("realm_id"),
        col("payment_provider"),
        col("source_currency").as("currency"),
        col("amount"),
        col("amount_usd"),
        col("ts")
      )

    if (!cli.dryRun) {
      revenue
        .write
        .redshift(spark)
        .option("dbtable", write_dbtable)
        .mode(SaveMode.Append)
        .save()
    } else {
      revenue.show()
    }
 }

  /**
    * A condition which selects the closest weekday
    *
    * If the day is Sunday, it will select Friday
    * If the day is Saturday, it will select Friday
    * Otherwise, select the current day
    */
  private def whenWeekday(df1: DataFrame, df2: DataFrame): Column = {
    if (!df1.dtypes.exists(dtype => dtype._1 == "ts_date")) {
      throw new Exception("Data frame 1 does not have the column ts_date")
    }
    if (!df1.dtypes.exists(dtype => dtype._1 == "ts_weekday")) {
      throw new Exception("Data frame 1 does not have the column ts_weekday")
    }
    if (!df2.dtypes.exists(dtype => dtype._1 == "date")) {
      throw new Exception("Data frame 2 does not have the column date")
    }

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
