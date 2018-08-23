import com.diceplatform.brain._
import com.diceplatform.brain.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object EventDateBucket extends Main {
  def main(args: Array[String]): Unit = {
    val parser = defaultParser

    parser.head(
      """Extract-Transform-Load (ETL) task for events
        |
        |Parses all events from JSON objects, sorts per date and inserts into that date file.
      """.stripMargin
    )

    var cli: Config = Config()
    parser.parse(args, cli) match {
      case Some(c) => cli = c
      case None => System.exit(1)
    }

    val events = spark.read.jsonSingleLine(spark, cli.path, Schema.root)

    val events_count = events.count()

    val df = events.withColumn("ts", to_utc_timestamp(col("ts"), "yyyy-MM-dd hh:mm:ss"))
      .withColumn("year", date_format(col("ts"), "yyyy" ))
      .withColumn("month", date_format(col("ts"), "MM" ))
      .withColumn("day", date_format(col("ts"), "dd" ))
      .withColumn("hour", date_format(col("ts"), "HH" ))

    df.show()

    if (!cli.dryRun) {
      // write to files
      print ("writing")
      df.write
        .mode("overwrite")
        .format("json")
        .partitionBy("year", "month", "day", "hour")
        .save("/tmp/output")
      print("done")
    } else {
      df.show()
    }
  }
}
