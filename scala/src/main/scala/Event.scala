import com.diceplatform.brain._
import com.diceplatform.brain.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Event extends Main {
  def main(args: Array[String]): Unit = {
    val parser = defaultParser

    parser.head(
      """Extract-Transform-Load (ETL) task for events
        |
        |Parses EVENT_WENT_LIVE and EVENT_WENT_NOTLIVE events from JSON objects stored in files and appends to the event table
      """.stripMargin
    )

    var cli: Config = Config()
    parser.parse(args, cli) match {
      case Some(c) => cli = c
      case None => System.exit(1)
    }

    val events = spark.read.jsonSingleLine(spark, cli.path, Schema.root)

    val events_count = events.count()
    // TODO: Add support for stream events

    //
    //                 Table "public.event"
    //   Column  |  Type   | Collation | Nullable | Default
    // ----------+---------+-----------+----------+---------
    //  event_id | integer |           | not null |
    //  realm_id | integer |           | not null |
    // Indexes:
    //     "event_pkey" PRIMARY KEY, btree (event_id)
    //
    val df = events.where(col("payload.data.ta").isin(ActionType.EVENT_WENT_LIVE, ActionType.EVENT_WENT_NOTLIVE))

    val updates = df.join(realms, df.col("realm") === realms.col("name"))

    val updates_count =  updates.count()
    print("-----total------",events_count,"-----events------", updates_count)

    val updates_live =  updates
            .where(col("payload.data.ta") === ActionType.EVENT_WENT_LIVE)
            .select(
              col("payload.data.DGE_EVENT_ID").alias("event_id"),
              col("realm_id"),
              col("ts").alias("start_at")
            )
    val live_count = updates_live.count()
    val updates_not_live =  updates
        .where(col("payload.data.ta") === ActionType.EVENT_WENT_NOTLIVE)
        .select(
          col("payload.data.DGE_EVENT_ID").alias("event_id"),
          col("realm_id"),
          col("ts").alias("finish_at")
        )
    val notlive_count= updates_not_live.count()

    print("-----total------",events_count,"-----live events------", live_count,"-----notlive events------", notlive_count)
    if (!cli.dryRun) {
     updates_live
            .write
            .redshift(spark)
            .mode(SaveMode.Append)
            .option("dbtable", "event_start")
            .save()

     updates_not_live
        .write
        .redshift(spark)
        .mode(SaveMode.Append)
        .option("dbtable", "event_finish")
        .save()
    } else {
      updates.show()
    }
  }
}
