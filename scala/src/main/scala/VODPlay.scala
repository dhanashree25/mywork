import com.diceplatform.brain._
import com.diceplatform.brain.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object VoDPlay extends Main {
  def main(args: Array[String]): Unit = {
    val parser = defaultParser
    parser.head(
      """Extract-Transform-Load (ETL) task for calculating video-on-demand (VOD) plays
        |
        |Parses VOD_PROGRESS events from JSON objects stored in files and appends to the VOD play table
      """.stripMargin
    )

    var cli: Config = Config()
    parser.parse(args, cli) match {
      case Some(c) => cli = c
      case None => System.exit(1)
    }

    val events = spark.read.jsonSingleLine(spark, cli.path, Schema.root)

    // TODO: Add support for stream events
    val df = events.where(col("payload.action") === Action.VOD_PROGRESS)
      .join(realms, df.col("realm") === realms.col("name"), "left_outer")

    val newSessions = df
      .select(collect_set(col("payload.cid")).as("session_ids"))
      .first()
      .getList[String](0)
      .toArray()

    val plays = spark.read.redshift(spark)
      .option("dbtable", "vod_play")
      .load()
      .where(col("session_id").isin(newSessions:_*))

    val updates = df
      .filter(col("realm_id").isNotNull)
      .select(
        col("realm_id"),
        col("payload.cid").alias("session_id"),
        col("customerId").alias("customer_id"),
        col("payload.video").alias("video_id"),
        col("payload.progress").alias("duration"),
        to_timestamp(col("payload.data.startedAt") / 1000).alias("started_at"),
        col("ts").alias("start_at"),
        col("ts").alias("end_at"),
        col("country"),
        col("town")
      )
      .union(plays)
      .groupBy(
        col("realm_id"),
        col("session_id"), // TODO: Consider removing
        col("customer_id"),
        col("video_id"),
        col("started_at"),
        col("country"),
        col("town")
      )
      .agg(
        max("duration").alias("duration"),
        min("start_at").alias("start_at"),
        max("end_at").alias("end_at")
      )

    print("-----total------" + events.count() + "-----payments------" + updates.count())

    val misseddf = df.filter(col("realm_id").isNull)
    print("-----Missed VoD plays------" + misseddf.count())
    misseddf.collect.foreach(println)

    if (!cli.dryRun) {
          print("Writing to table")
          updates
            .write
            .redshift(spark)
            .option("dbtable", "vod_play")
            .mode(SaveMode.Append)
            .save()
    } else {
      updates.show()
    }
  }
}
