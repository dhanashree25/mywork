import com.diceplatform.brain._

import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Analytics").getOrCreate()

//    val path = "/Users/iszak.bryan/Workspace/Analytics/data/prod/2018/06/11/01/*"
    val path = "s3a://dce-tracking/prod/2018/05/05/*/*"
//    val path = "s3a://dce-tracking/prod/2018/05/01/*/*"

    val rdd = spark.sparkContext.
      textFile(path).
      map(_.replace("}{", "}\n{")). // TODO: Prone to breakage
      flatMap(_.split("\n"))

    val ds = spark.createDataset(rdd)(Encoders.STRING)

    val df = spark.read.
      option("allowSingleQuotes", false).
      option("multiLine", false).
      schema(Schema.root).
      json(ds)

    // Merge ta and TA columns
    spark.sql("set spark.sql.caseSensitive=true")
    NormalizeActionTypes.transform(df)

    //  NormalizeActionTypes.transform(df).select("payload.data.ta").show()

    val a = df.where(expr(s"payload.data.ta == '${ActionType.REGISTER_USER}'")).select(count(lit(1)))
    a.explain()
    a.show()
  }
}