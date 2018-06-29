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
    val parser = new scopt.OptionParser[Config]("scopt") {
      head(
        """Extract-Transform-Load (ETL) task for user_logins table
          |
          |Parses USER_SIGN_IN events from and inserts into user_logins table
        """.stripMargin
        )

      opt[String]('p', "path")
        .action((x, c) => c.copy(path = x) )
        .text("path to files, local or remote")
        .required()

      opt[Boolean]('d', "dry-run")
        .action((x, c) => c.copy(dryRun = x) )
        .text("dry run")
    }

    var cli: Config = Config()
    parser.parse(args, cli) match {
      case Some(c) => cli = c
      case None => System.exit(1)
    }

    val events = spark.read.jsonSingleLine(spark, cli.path, Schema.root)
    
    val realms = spark
      .read
      .redshift(spark)
      .option("dbtable", "realm")
      .load()
      
//                    Table "public.user_logins"
//       Column    |            Type             | Collation | Nullable | Default 
//    -------------+-----------------------------+-----------+----------+---------
//     customer_id | character varying(25)       |           |          | 
//     realm       | character varying(50)       |           |          | 
//     town        | character varying(100)      |           |          | 
//     country     | character varying(100)      |           |          | 
//     client_ip   | character varying(50)       |           |          | 
//     type        | character varying(50)       |           |          | 
//     device      | character varying(25)       |           |          | 
//     ts          | timestamp without time zone |           |          | 
//    COMPOUND SORTKEY(ts, realm, device, country)

    spark.sql("set spark.sql.caseSensitive=true")
      
    val logindf = events.where(col("payload.action") === Action.USER_SIGN_IN)
            .select(
              expr("realm"),
              col("customerId").alias("customer_id"),
              col("country"),
              col("town"),
              col("ts"),
              col("clientIp").alias("client_ip"),
              expr("payload.data.device as device")
            )
            
    print("-----total------"+events.count()+"-----logins------"+ logindf.count())
    
    if (!cli.dryRun) {
       if (logindf.count()> 0){
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