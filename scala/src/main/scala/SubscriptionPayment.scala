import com.diceplatform.brain._
import com.diceplatform.brain.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


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

    val df = events.where(col("payload.data.TA") === ActionType.SUCCESSFUL_PURCHASE)
      .join(realms, col("realm") === realms.col("name"), "left_outer")

    val updates = df.filter(col("realm_id").isNotNull)
                    .select(
                      col("customerId").alias("customer_id"),
                      col("realm_id"),
                      col("town"),
                      col("country"),
                      col("ts"),
                      col("payload.data.PAYMENT_PROVIDER").alias("payment_provider"),
                      col("payload.data.PRICE_WITH_TAX_AMOUNT").alias("amount_with_tax"),
                      col("payload.data.PRICE_WITH_TAX_CURRENCY").alias("currency"),
                      col("payload.data.IS_TRIAL").alias("is_trial"),
                      col("payload.data.TRIAL_DAYS").alias("trial_days"),
                      col("payload.data.SKU").alias("sku"),
                      col("payload.data.REVOKED").alias("revoked"),
                      col("payload.data.CANCELLED").alias("cancelled")
                    )
                    .distinct()
                    .withColumn("payment_id" , monotonically_increasing_id)
                    .cache()

    val payments = updates
                     .select(
                        col("customer_id"),
                        col("realm_id"),
                        col("town"),
                        col("country"),
                        col("ts"),
                        col("payment_provider"),
                        col("amount_with_tax").cast(IntegerType), // To-do: check if need float
                        col("currency"),
                        col("sku"),
                        col("payment_id"))

    val subscriptions = updates
                     .select(
                        col("customer_id"),
                        col("realm_id"),
                        col("town"),
                        col("country"),
                        col("ts"),
                        col("payment_id"),
                        col("is_trial"),
                        col("trial_days"),
                        col("sku"),
                        col("revoked"),
                        col("cancelled"))

    print("-----total------" + events_count + "-----payments------" + updates.count())

    val misseddf = {
      df.filter(col("realm_id").isNull)
    }
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
