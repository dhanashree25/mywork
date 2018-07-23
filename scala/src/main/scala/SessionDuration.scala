import com.diceplatform.brain._
import com.diceplatform.brain.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object SessionDuration extends Main {
  def main(args: Array[String]): Unit = {
    val parser = defaultParser

    parser.head(
      """Extract-Transform-Load (ETL) task for calculating session durations
        |
        |Parses all events from JSON objects stored in files and appends to the session_duration table
      """.stripMargin
    )

    var cli: Config = Config()
    parser.parse(args, cli) match {
      case Some(c) => cli = c
      case None => System.exit(1)
    }

    val events = spark.read.jsonSingleLine(spark, cli.path, Schema.root)

    // TODO: Add support for stream events
    val vod_df = events.where(col("payload.action") === Action.VOD_PROGRESS)
    val live_df = events.where(col("payload.action") === Action.LIVE_WATCHING)

    /**
                               Table "public.session_duration"
        Column    |            Type             | Collation | Nullable | Default
     -------------+-----------------------------+-----------+----------+---------
      realm_id    | integer                     |           | not null |
      session_id  | character varying(64)       |           | not null |
      customer_id | character varying(32)       |           | not null |
      duration    | integer                     |           | not null |
      started_at  | timestamp without time zone |           | not null |
      start_at    | timestamp without time zone |           | not null |
      end_at      | timestamp without time zone |           | not null |
      country     | character(2)                |           | not null |
      town        | character varying(1024)     |           | not null |

      */

    val newSessions = events
      .where(col("payload.action") === Action.VOD_PROGRESS)
      .select(collect_set(col("payload.cid")).as("session_ids"))
      .first()
      .getList[String](0)
      .toArray()        //This will need optimisation

    val previousSessions = spark.read.redshift(spark)
      .option("dbtable", "session_duration")
      .load()
      .where(col("session_id").isin(newSessions:_*)) // this will need to be optimised

    // progress for vod is more significant than calculated duration
    val vod_updates = vod_df
      .join(realms, vod_df.col("realm") === realms.col("name"))
      .select(
        col("realm_id"),
        col("payload.cid").alias("session_id"),
        col("customerId").alias("customer_id"),
        col("payload.progress").alias("duration"),
        to_timestamp(col("payload.data.startedAt") / 1000).alias("started_at"),
        col("ts").alias("start_at"),
        col("ts").alias("end_at"),
        col("country"),
        col("town")
      )
      .union(previousSessions)
      .groupBy(
        col("realm_id"),
        col("session_id"), // TODO: Consider removing
        col("started_at"),
        col("customer_id"),
        col("country"),
        col("town")
      )
      .agg(
        max("duration").alias("duration"),
        min("start_at").alias("start_at"),
        max("end_at").alias("end_at")
      )

    // Because live_actions doesnt have progress so we calculate
    val live_updates = live_df
      .join(realms, live_df.col("realm") === realms.col("name"))
      .select(
        col("realm_id"),
        col("payload.cid").alias("session_id"),
        col("customerId").alias("customer_id"),
        col("payload.progress").alias( "duration"),
        to_timestamp(col("payload.data.startedAt") / 1000).alias("started_at"),
        col("ts").alias("start_at"),
        col("ts").alias("end_at"),
        col("country"),
        col("town")
      )
      .union(previousSessions)
      .groupBy(
        col("realm_id"),
        col("session_id"), // TODO: Consider removing
        col("started_at"),
        col("customer_id"),
        col("country"),
        col("town")
      )
      .agg(
        max("duration").alias("duration"),
        min("start_at").alias("start_at"),
        max("end_at").alias("end_at")
      )

    val updates = vod_updates
      .union(live_updates.withColumn("duration",unix_timestamp(col("end_at"))-unix_timestamp(col("start_at"))))

    if (!cli.dryRun) {
          print("Writing to table")
           updates
            .write
            .redshift(spark)
            .option("dbtable", "session_duration")
            .mode(SaveMode.Append)
            .save()
    } else {
      updates.show()
    }
  }
}
