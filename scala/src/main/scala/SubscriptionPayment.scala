import com.diceplatform.brain._
import com.diceplatform.brain.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object SubscriptionPayment extends Main {
  def main(args: Array[String]): Unit = {
    val parser = defaultParser

    parser.head(
      """Extract-Transform-Load (ETL) task for payments and subscriptions
        |
        |Parses SUCCESSFUL_PURCHASE events from JSON objects stored in files and appends to
        |the payment and subscription tables
      """.stripMargin
    )

    var cli: Config = Config()
    parser.parse(args, cli) match {
      case Some(c) => cli = c
      case None => System.exit(1)
    }

    val events = spark.read.jsonSingleLine(spark, cli.path, Schema.root)

    val events_count = events.count()

    val df_purchase = events.where(col("payload.data.TA") === ActionType.SUCCESSFUL_PURCHASE)

    val df = df_purchase
      .join(realms, df_purchase.col("realm") === realms.col("name"), "left_outer").cache()

    val updates = df
                    .filter(col("realm_id").isNotNull)
                    .select(
                        col("realm_id"),
                        col("customerId").alias("customer_id"),
                        col("country"),
                        col("town"),
                        col("ts"),
                        col("payload.data.PAYMENT_PROVIDER").alias("payment_provider"),
                        col("payload.data.PRICE_WITH_TAX_AMOUNT").alias("amount_with_tax"),
                        col("payload.data.PRICE_WITH_TAX_CURRENCY").alias("currency"),
                        col("payload.data.IS_TRIAL").alias("is_trial"),
                        col("payload.data.TRIAL_DAYS").alias("trial_days"),
                        col("payload.data.SKU").alias("sku"),
                        col("payload.data.REVOKED").alias("revoked"),
                        col("payload.data.CANCELLED").alias("cancelled")
                    ).withColumn("payment_id" , monotonically_increasing_id)

    val payments = updates
                     .select(
                        col("realm_id"),
                        col("customer_id"),
                        col("country"),
                        col("town"),
                        col("ts"),
                        col("payment_provider"),
                        col("amount_with_tax"),
                        col("currency"),
                        col("sku"),
                        col("payment_id"))

    val subscriptions = updates
                     .select(
                        col("realm_id"),
                        col("customer_id"),
                        col("country"),
                        col("town"),
                        col("ts"),
                        col("sku"),
                        col("payment_id"),
                        col("revoked"),
                        col("cancelled"),
                        col("is_trial"),
                        col("trial_days"))

    print("-----total------" + events_count + "-----payments------" + updates.count())

    val misseddf = df.filter(col("realm_id").isNull)
    print("-----Missed Payments------" + misseddf.count() )
    misseddf.collect.foreach(println)

    if (!cli.dryRun) {
      payments
          .write
          .redshift(spark)
          .mode(SaveMode.Append)
          .option("dbtable", "payment")
          .save()

       subscriptions
          .write
          .redshift(spark)
          .mode(SaveMode.Append)
          .option("dbtable", "subscription")
          .save()
    } else {
      updates.show()

    }
  }
}
