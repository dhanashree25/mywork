import com.diceplatform.brain._
import com.diceplatform.brain.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

case class DateBucketConfig(path: String = "", dryRun: Boolean = false, dateBucket: String = "")

object EventDateBucket extends Main {
  def main(args: Array[String]): Unit = {
    lazy val bucketParser: scopt.OptionParser[DateBucketConfig] = {
      new scopt.OptionParser[DateBucketConfig]("scopt") {
        opt[String]("path")
          .action((x, c) => c.copy(path = x) )
          .text("path to files, local or remote")
          .required()

        opt[Boolean]("dry-run")
          .action((x, c) => c.copy(dryRun = x) )
          .optional()
          .text("dry run")

        opt[String]("date-bucket")
          .action((x, c) => c.copy(dateBucket = x) )
          .text("path to date bucket, required only for EventDateBucket job")
          .optional()
      }
    }
    val parser = bucketParser

    parser.head(
      """Extract-Transform-Load (ETL) task for events
        |
        |Parses all events into date based buckets
      """.stripMargin
    )

    var cli: DateBucketConfig = DateBucketConfig()
    parser.parse(args, cli) match {
      case Some(c) => cli = c
      case None => System.exit(1)
    }

    val events = spark.read.jsonSingleLine(spark, cli.path, Schema.root)

    val df = events.join(realms, col("realm") === realms.col("name"), "left_outer")

    val total = df.count()

    val misseddf = {
      df.filter(col("realm_id").isNull)
    }
    print("-----Missed Payments------" + misseddf.count() )
    misseddf.collect.foreach(println)

    val updates = df.filter(col("realm_id").isNotNull)
      .withColumn("bucket_ts", to_utc_timestamp(col("ts"), "yyyy-MM-dd hh:mm:ss"))
      .withColumn("date", date_format(col("bucket_ts"), "yyyy-MM-dd"))
      .withColumn("year", date_format(col("bucket_ts"), "yyyy" ))
      .withColumn("bucket.month", date_format(col("bucket_ts"), "MM" ))
      .withColumn("bucket.day", date_format(col("bucket_ts"), "dd" ))
      .withColumn("bucket.hour", date_format(col("bucket_ts"), "HH" )).cache()

    val counts = updates.groupBy("date").agg(count("date")).orderBy("date")

    println("------------Total------------" + total)

    println(" --------------Distinct dates from processed day---------")
    counts.collect.foreach(println)

    if (!cli.dryRun) {
      print("Writing to table")
        updates.write
          .mode("append")
          .format("json")
          .partitionBy("year", "month", "day", "hour")
          .save(cli.dateBucket)
    } else {
      updates.show()
    }
  }
}
