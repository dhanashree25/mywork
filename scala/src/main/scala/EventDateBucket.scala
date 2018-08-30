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
        |Parses all events into date based buckets
      """.stripMargin
    )

    var cli: Config = Config()
    parser.parse(args, cli) match {
      case Some(c) => cli = c
      case None => System.exit(1)
    }

    val df = spark.read.jsonSingleLine(spark, cli.path, Schema.root)

    val total = df.count()

    val updates = df.withColumn("ts", to_utc_timestamp(col("ts"), "yyyy-MM-dd hh:mm:ss"))
      .withColumn("date", date_format(col("ts"), "yyyy-MM-dd"))
      .withColumn("year", date_format(col("ts"), "yyyy" ))
      .withColumn("month", date_format(col("ts"), "MM" ))
      .withColumn("day", date_format(col("ts"), "dd" ))
      .withColumn("hour", date_format(col("ts"), "HH" )).cache()

    val counts = updates.groupBy("date").agg(count("date")).orderBy("date")

    println(" --------------Distinct dates from processed day---------")
    counts.collect.foreach(println)

    if (!cli.dryRun) {
      print("Writing to table")
        updates.write
          .mode("append")
          .format("json")
          .partitionBy("year", "month", "day", "hour")
          .save("s3n://dce-spark-data-prod/")
    } else {
      counts.collect.foreach(println)
      print (total)
    }
  }
}
