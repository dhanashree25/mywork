import com.diceplatform.brain._
import com.diceplatform.brain.implicits._

import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.hadoop.io._


object Logins {
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder.appName("Analytics").getOrCreate()
    val sc = spark.sparkContext
    
    val parser = new scopt.OptionParser[Config]("scopt") {
      head(
        """Extract-Transform-Load (ETL) task for user_logins table
          |
          |Parses USER_SIGN_IN events from and inserts into user_logins table
        """.stripMargin)

      opt[String]('p', "path").action( (x, c) =>
        c.copy(path = x) ).text("path to files")
        }

    var path: String = ""
      parser.parse(args, Config()) match {
        case Some(c) => path = c.path
        case None => System.exit(1)
      }

    // Parse single-line multi-JSON object into single-line single JSON object
     val rdd = sc.hadoopFile(path, classOf[SingleJSONLineInputFormat], classOf[LongWritable], classOf[Text])
      .map(pair => pair._2.toString)
      .setName(path)
  
    val df = spark.read
      .schema(Schema.root)
      .json(spark.createDataset(rdd)(Encoders.STRING))
        
//                         Table "public.user_logins"
//   Column    |            Type             | Collation | Nullable | Default 
//-------------+-----------------------------+-----------+----------+---------
// customer_id | character varying(25)       |           |          | 
// realm       | character varying(50)       |           |          | 
// town        | character varying(100)      |           |          | 
// country     | character varying(100)      |           |          | 
// client_ip   | character varying(50)       |           |          | 
// device      | character varying(50)       |           |          | 
// ts          | timestamp without time zone |           |          | 

    spark.sql("set spark.sql.caseSensitive=true")
      
      
    val logindf = df.where(col("payload.action") === Action.USER_SIGN_IN)
            .select(
              expr("realm"),
              col("customerId").alias("customer_id"),
              col("country"),
              col("town"),
              col("ts"),
              col("clientIp").alias("client_ip"),
              expr("payload.data.device as device")
            )
    print("-----total------",df.count(),"-----logins------", logindf.count())
    if (logindf.count()> 0){
          logindf.write
            .format("jdbc")
            .mode(SaveMode.Append)
            .option("driver", spark.conf.get("spark.jdbc.driver", "com.amazon.redshift.jdbc.Driver"))
            .option("url", spark.conf.get("spark.jdbc.url"))
            .option("user", spark.conf.get("spark.jdbc.username"))
            .option("password", spark.conf.get("spark.jdbc.password"))
            .option("dbtable", "user_logins")
            .save()
    }
  }
}