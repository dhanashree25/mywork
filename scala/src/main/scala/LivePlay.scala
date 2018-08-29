import com.diceplatform.brain._
import com.diceplatform.brain.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object LivePlay extends Main {
  def main(args: Array[String]): Unit = {
    val parser = defaultParser

    parser.head(
      """Extract-Transform-Load (ETL) task for calculating Live video play
        |
        |Parses LIVE_ACTION from JSON objects stored in files and appends to the live_play table
      """.stripMargin
    )

    var cli: Config = Config()
    parser.parse(args, cli) match {
      case Some(c) => cli = c
      case None => System.exit(1)
    }

    val events_raw = spark.read.jsonSingleLine(spark, cli.path, Schema.root)

    val events = events_raw.where(col("payload.action") === Action.LIVE_WATCHING).cache()

    val df = events.filter(col("payload.cid").isNotNull and col("payload.cid")=!="")
      .join(realms, col("realm") === realms.col("name"), "left_outer").cache()

    val newSessions = df
      .select(collect_set(col("payload.cid")).as("session_ids"))
      .first()
      .getList[String](0)
      .toArray()        //This will need optimisation

    val previousPlays = spark.read.redshift(spark)
      .option("dbtable", "live_play")
      .load()
      .where(col("session_id").isin(newSessions:_*)) // this will need to be optimised

    val live_updates = df
      .filter(col("realm_id").isNotNull)
      .select(
        col("realm_id"),
        col("payload.cid").alias("session_id"),
        col("customerId").alias("customer_id"),
        col("payload.video").alias("video_id"),
        col("payload.data.device").alias( "device"),
        to_timestamp(col("payload.data.startedAt") / 1000).alias("started_at"),
        col("ts").alias("start_at"),
        col("ts").alias("end_at"),
        col("country"),
        col("town")
      )
      .groupBy(
        col("realm_id"),
        col("session_id"), // TODO: Consider removing
        col("started_at"),
        col("customer_id"),
        col("video_id"),
        col("device"),
        col("country"),
        col("town")
      )
      .agg(
        min("start_at").alias("start_at"),
        max("end_at").alias("end_at")
      )
      .withColumn("duration",unix_timestamp(col("end_at"))-unix_timestamp(col("start_at")))

    val reorderedColumnNames = List("realm_id",
                                                 "session_id",
                                                 "customer_id",
                                                 "video_id",
                                                 "device",
                                                 "duration",
                                                 "started_at",
                                                 "start_at",
                                                 "end_at",
                                                 "country",
                                                 "town")

    val final_df = live_updates.select(reorderedColumnNames.head, reorderedColumnNames.tail: _*).union(previousPlays)
      .groupBy(
        col("realm_id"),
        col("session_id"), // TODO: Consider removing
        col("started_at"),
        col("customer_id"),
        col("video_id"),
        col("device"),
        col("country"),
        col("town")
      )
      .agg(
        max("duration").alias("duration"),
        min("start_at").alias("start_at"),
        max("end_at").alias("end_at")
      )

    val updates = final_df.select(reorderedColumnNames.head, reorderedColumnNames.tail: _*).cache()

    val updates_count=  updates.count()
    print("-----total------",events.count(),"-----live------", updates_count)

    val misseddf = df
      .filter(col("realm_id").isNull)
    println("-----Missed Live events------" + misseddf.count() )
    misseddf.collect.foreach(println)

    val invalid_sessions = events.filter(col("payload.cid").isNull or col("payload.cid")==="")
    println("---------Invalid Sessions---------" + invalid_sessions.count())
    invalid_sessions.collect.foreach(println)


    if (!cli.dryRun) {
          print("Writing to table")
           updates
            .write
            .redshift(spark)
            .option("dbtable", "live_play")
            .mode(SaveMode.Append)
            .save()
    } else {
      updates.collect.foreach(println)
    }
  }
}
