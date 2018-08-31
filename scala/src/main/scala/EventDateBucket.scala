import SubscriptionPayment.{realms, spark}
import com.diceplatform.brain._
import com.diceplatform.brain.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object EventDateBucket extends Main {
  def main(args: Array[String]): Unit = {
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
      .withColumn("bucket.ts", to_utc_timestamp(col("ts"), "yyyy-MM-dd hh:mm:ss"))
      .withColumn("bucket.date", date_format(col("ts"), "yyyy-MM-dd"))
      .withColumn("bucket.year", date_format(col("ts"), "yyyy" ))
      .withColumn("bucket.month", date_format(col("ts"), "MM" ))
      .withColumn("bucket.day", date_format(col("ts"), "dd" ))
      .withColumn("bucket.hour", date_format(col("ts"), "HH" )).cache()

    val counts = updates.groupBy("date").agg(count("date")).orderBy("date")

    println("------------Total------------" + total)

    println(" --------------Distinct dates from processed day---------")
    counts.collect.foreach(println)

    if (!cli.dryRun) {
      print("Writing to table")
        updates.write
          .mode("append")
          .format("json")
          .partitionBy("bucket.year", "bucket.month", "bucket.day", "bucket.hour")
          .save(cli.dateBucket)
    } else {
      updates.show()
    }
  }
}
