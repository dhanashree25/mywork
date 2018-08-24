import com.diceplatform.brain._
import com.diceplatform.brain.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.hadoop.io._


object Logins extends Main {
  def main(args: Array[String]): Unit = {
    val parser = defaultParser

    parser.head(
      """Extract-Transform-Load (ETL) task for user_logins table
        |
        |Parses USER_SIGN_IN events from and inserts into user_logins table
      """.stripMargin
    )

    var cli: Config = Config()
    parser.parse(args, cli) match {
      case Some(c) => cli = c
      case None => System.exit(1)
    }

    val events = spark.read.jsonSingleLine(spark, cli.path, Schema.root)

    val event_count = events.count()

    val df = events.where(col("payload.action") === Action.USER_SIGN_IN)
      .join(realms, col("realm") === realms.col("name"), "left_outer").cache()

    val logindf =df
              .filter(col("realm_id").isNotNull)
              .withColumn("is_success",when(col("payload.data.TA")===ActionType.SUCCESSFUL_LOGIN, true).otherwise(false))
              .select(
                col("realm_id"),
                col("town"),
                col("country"),
                col("clientIp").alias("client_ip"),
                col("payload.data.device").alias("device"),
                col("ts"),
                col("is_success"),
                col("customerId").alias("customer_id")
              )

    val login_count = logindf.count()
    print("-----total------" + event_count + "-----logins------" + login_count)

    val misseddf = df.filter(col("realm_id").isNull)
    print("-----Missed Logins------" + misseddf.count() )
    misseddf.collect.foreach(println)

    if (!cli.dryRun) {
       if (login_count> 0){
         logindf.write.
           redshift(spark)
           .option("dbtable", "user_logins")
           .mode(SaveMode.Append)
           .save()
       }
    } else {
      logindf.show()
    }
 }
}
