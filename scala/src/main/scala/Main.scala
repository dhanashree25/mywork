import com.diceplatform.brain._
import com.diceplatform.brain.implicits._

import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Analytics").getOrCreate()

    val path = "s3a://dce-tracking/prod/2018/05/05/*/*"

    // Parse single-line multi-JSON object into single-line single JSON object
    val df = spark.read
      .jsonSingleLine(spark, path, Schema.root)

    val a = df.where(col("payload.action") === ActionType.REGISTER_USER).select(count(lit(1)))
  }
}