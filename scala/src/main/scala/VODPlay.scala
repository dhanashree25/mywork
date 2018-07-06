import com.diceplatform.brain._
import com.diceplatform.brain.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object VoDPlay extends Main {
  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Config]("scopt") {
      head(
        """Extract-Transform-Load (ETL) task for calculating video-on-demand (VOD) plays
          |
          |Parses VOD_PROGRESS events from JSON objects stored in files and appends to the VOD play table
        """.stripMargin
      )

      opt[String]('p', "path")
        .action((x, c) => c.copy(path = x) )
        .text("path to files, local or remote")
        .required()

      opt[Boolean]('d', "dryRun")
        .action((x, c) => c.copy(dryRun = x) )
        .text("dry run")
    }

    var cli: Config = Config()
    parser.parse(args, cli) match {
      case Some(c) => cli = c
      case None => System.exit(1)
    }

    val events = spark.read.jsonSingleLine(spark, "s3a://dce-tracking/prod/2018/04/01/00/*", Schema.vodplay)

    // TODO: Add support for stream events

    //
    //  Table "public.realm"
    //   Column  |          Type          | Collation | Nullable | Default
    //  ---------+------------------------+-----------+----------+---------
    //  realm_id | integer                |           | not null |
    //  name     | character varying(256) |           | not null |
    //

    val realms = spark
      .read
      .redshift(spark)
      .option("dbtable", "realm")
      .load()

    /**
                               Table "public.vod_play"
        Column    |            Type             | Collation | Nullable | Default
     -------------+-----------------------------+-----------+----------+---------
      realm_id    | integer                     |           | not null |
      session_id  | character varying(64)       |           | not null |
      customer_id | character varying(32)       |           | not null |
      video_id    | integer                     |           | not null |
      duration    | integer                     |           | not null |
      started_at  | timestamp without time zone |           | not null |
      start_at    | timestamp without time zone |           | not null |
      end_at      | timestamp without time zone |           | not null |
      country     | character(2)                |           | not null |
      town        | character varying(1024)     |           | not null |

      */

    /**
      * {
      * "payload": {
      *   "action": 2,
      *   "data": {
      *     "cid": "0f3360bagf17da",
      *     "startedAt": 1525162096616,
      *     "device": "ANDROID_PHONE",
      *     "TA": "OK"
      *   },
      *   "video": 10182,
      *   "progress": 2703,
      *   "cid": "0f3360bagf17da"
      * },
      * "country": "US",
      * "town": "Bozeman",
      * "clientIp": "174.45.109.251",
      * "realm": "dce.pbr",
      * "customerId": "fTX_",
      * "ts": "2018-05-01 16:14:00"
      * }
      */

    val df = events.where(col("payload.action") === Action.VOD_PROGRESS)


    val newSessions = events
      .where(col("payload.action") === Action.VOD_PROGRESS)
      .select(collect_set(col("payload.cid")).as("session_ids"))
      .first()
      .getList[String](0)
      .toArray()
     
    print(newSessions)

    val plays = spark.read.redshift(spark)
      .option("dbtable", "vod_play")
      .load()
      .where(col("session_id").isin(newSessions:_*))

    val updates = df
      .join(realms, df.col("realm") === realms.col("name"))
      .select(
        col("realm_id"),
        col("payload.cid").alias("session_id"),
        col("customerId").alias("customer_id"),
        col("payload.video").alias("video_id"),
        col("payload.progress").alias("duration"),
        to_timestamp(col("payload.data.startedAt") / 1000).alias("started_at"),
        col("ts").alias("start_at"),
        col("ts").alias("end_at"),
        col("town"),
        col("country")
      )
      .union(plays)
      .groupBy(
        col("realm_id"),
        col("session_id"), // TODO: Consider removing
        col("customer_id"),
        col("video_id"),
        col("started_at")
      )
      .agg(
        max("duration").alias("duration"),
        max("start_at").alias("start_at"),
        min("end_at").alias("end_at")
      )

    if (!cli.dryRun) {
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