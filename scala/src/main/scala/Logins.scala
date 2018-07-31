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

//                               Table "public.user_logins"
//         Column    |            Type             | Collation | Nullable | Default 
//      -------------+-----------------------------+-----------+----------+---------
//       customer_id | character varying(25)       |           | not null | 
//       realm_id    | integer                     |           | not null | 
//       town        | character varying(100)      |           |          | 
//       country     | character(2)                |           |          | 
//       client_ip   | character varying(50)       |           |          | 
//       type        | character varying(50)       |           |          | 
//       device      | character varying(25)       |           |          | 
//       ts          | timestamp without time zone |           | not null | 
//       is_success  | boolean                     |           |          | 
//       Foreign-key constraints:
//          "test_user_logins_country_fkey" FOREIGN KEY (country) REFERENCES country(alpha_2)
//          "test_user_logins_realm_id_fkey" FOREIGN KEY (realm_id) REFERENCES realm(realm_id)
//       COMPOUND SORTKEY(ts, realm_id, device, country)

    spark.sql("set spark.sql.caseSensitive=true")

    val logindf = events.where(col("payload.action") === Action.USER_SIGN_IN)
              .join(realms, events.col("realm") === realms.col("name"))
              .withColumn("is_success",when(col("payload.data.TA")===ActionType.SUCCESSFUL_LOGIN, true).otherwise(false))
              .select(
              col("realm_id"),
              col("customerId").alias("customer_id"),
              col("country"),
              col("town"),
              col("ts"),
              col("clientIp").alias("client_ip"),
              col("payload.data.device").alias("device"),
              col("is_success")
              )

   val login_count = logindf.count()
   print("-----total------" + event_count + "-----logins------" + login_count)

    if (!cli.dryRun) {
       if (login_count> 0){
          logindf
            .write
            .redshift(spark)
            .mode(SaveMode.Append)
            .option("dbtable", "user_logins")
            .save()
       }
    } else {
      logindf.show()
    }
 }
}
