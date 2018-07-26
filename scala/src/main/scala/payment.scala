import com.diceplatform.brain._
import com.diceplatform.brain.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Payment extends Main {
  def main(args: Array[String]): Unit = {
    val parser = defaultParser

    parser.head(
      """Extract-Transform-Load (ETL) task for payments
        |
        |Parses SUCCESSFUL_PURCHASE events from JSON objects stored in files and appends to the payment table
      """.stripMargin
    )

    var cli: Config = Config()
    parser.parse(args, cli) match {
      case Some(c) => cli = c
      case None => System.exit(1)
    }

    val events = spark.read.jsonSingleLine(spark, cli.path, Schema.root)
    
    val events_count = events.count()

    //
    //                 Table "public.payments"
    //   Column  |  Type   | Collation | Nullable | Default
    // ----------+---------+-----------+----------+---------
    //  event_id | integer |           | not null |
    //  realm_id | integer |           | not null |
    // Indexes:
    //     "event_pkey" PRIMARY KEY, btree (event_id)
    //
    val payments = events.where(col("payload.data.TA") === "SUCCESSFUL_PURCHASE")

    val updates = payments
                    .join(realms, payments.col("realm") === realms.col("name"))
                    .select(
                        col("realm_id"),
                        col("customerId").alias("customer_id"),
                        col("country"),
                        col("town"),
                        col("ts"),
                        col("payload.data.PAYMENT_PROVIDER").alias("payment_provider"),
                        col("payload.data.PRICE_WITH_TAX_AMOUNT").alias("amount_with_tax"),
                        col("payload.data.PRICE_WITH_TAX_CURRENCY").alias("currency")
                        )

    val updates_count=  updates.count()
    
    print("-----total------"+events_count+"-----payments------"+updates_count)
    
    if (!cli.dryRun) {
       updates
          .write
          .redshift(spark)
          .mode(SaveMode.Append)
          .option("dbtable", "payment")
          .save()
    } else {
      updates.show()
      
    }
  }
}
