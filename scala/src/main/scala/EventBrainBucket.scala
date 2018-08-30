import com.diceplatform.brain._
import com.diceplatform.brain.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object EventBrainBucket extends Main {
  def main(args: Array[String]): Unit = {
    val parser = defaultParser

    parser.head(
      """Extract-Transform-Load (ETL) task for events
        |
        |Parses all events into date based files
      """.stripMargin
    )

    var cli: Config = Config()
    parser.parse(args, cli) match {
      case Some(c) => cli = c
      case None => System.exit(1)
    }

    val df = spark.read.jsonSingleLine(spark, cli.path, Schema.root)

    val total = df.count()

    print("-----total------", total)


    val updates = df.withColumn("ts", to_utc_timestamp(col("ts"), "yyyy-MM-dd hh:mm:ss"))
      .withColumn("year", date_format(col("ts"), "yyyy" ))
      .withColumn("month", date_format(col("ts"), "MM" ))
      .withColumn("day", date_format(col("ts"), "dd" ))
      .withColumn("hour", date_format(col("ts"), "HH" ))

    updates.write
      .mode("overwrite")
      .format("json")
      .partitionBy("year", "month", "day", "hour")
      .save("s3://dce-spark-data-prod/")
  }
}
