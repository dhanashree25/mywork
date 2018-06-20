import com.diceplatform.brain._
import com.diceplatform.brain.implicits._

import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Analytics").getOrCreate()

    val path = "s3a://dce-tracking/prod/2018/05/05/*/*"

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

    val a = df.where(col("action") === ActionType.REGISTER_USER).select(count(lit(1)))
  }
}