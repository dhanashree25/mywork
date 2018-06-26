import com.diceplatform.brain._
import com.diceplatform.brain.implicits._

import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object Signup {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Analytics").getOrCreate()

    val path = "s3n://dce-tracking/prod/2018/06/23/*/*"
    print (path)

    // Parse single-line multi-JSON object into single-line single JSON object
      val rdd = spark.sparkContext
        .textFile(path)
        .map(_.replace("}{", "}\n{")) // TODO: Prone to breakage
        .flatMap(_.split("\n"))
  
      // TODO: Investigate Encoders
      val ds = spark.createDataset(rdd)(Encoders.STRING)
  
      val df = spark.read
        .option("allowSingleQuotes", false)
        .option("multiLine", false)
        .schema(Schema.root)
        .json(ds)
        .normalize()
      val df1= df.where(col("action") === ActionType.REGISTER_USER)
            .select(
              expr("realm"),
              col("customerId").alias("customer_id"),
              col("country"),
              col("town"),
              col("ts"),
              expr("payload.data.device as device")
            )
        df1.show()
    df1.write
          .format("jdbc")
          .mode(SaveMode.Append)
          .option("url", "jdbc:postgresql://172.21.105.71:5439/redshift?user=saffron&password=1Nn0v8t3")
          .option("driver", "com.amazon.redshift.jdbc.Driver")
          .option("dbtable", "signups")
          .option("user", "saffron")
          .option("password", "1Nn0v8t3")
          .save()
  }
}