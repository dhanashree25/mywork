import com.diceplatform.brain._
import com.diceplatform.brain.implicits._
import org.apache.hadoop.io._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Event extends Main {
  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Config]("scopt") {
      head(
        """Extract-Transform-Load (ETL) task for events
          |
          |Parses EVENT_WENT_LIVE and EVENT_WENT_NOTLIVE events from JSON objects stored in files and appends to the event table
        """.stripMargin
      )

      opt[String]('p', "path")
        .action((x, c) => c.copy(path = x) )
        .text("path to files, local or remote")
        .required()

      opt[Boolean]('d', "dry-run")
        .action((x, c) => c.copy(dryRun = x) )
        .text("dry run")
    }

    var cli: Config = Config()
    parser.parse(args, cli) match {
      case Some(c) => cli = c
      case None => System.exit(1)
    }

    val events = spark.read.jsonSingleLine(spark, cli.path, Schema.root)

    val event_count = events.count()
    // TODO: Add support for stream events

    val realms = spark
      .read
      .redshift(spark)
      .option("dbtable", "realm")
      .load()

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
    
    val updates_count=  updates.count()
    print("-----total------",events_count,"-----vod------", updates_count)
    
    if (!cli.dryRun) {
         updates
            .where(col("payload.data.ta") === ActionType.EVENT_WENT_LIVE)
            .select(
              col("payload.data.DGE_EVENT_ID").alias("event_id"),
              col("realm_id"),
              col("ts").alias("start_at")
            )
            .write
            .redshift(spark)
            .mode(SaveMode.Append)
            .option("dbtable", "event_start")
            .save()

      updates
        .where(col("payload.data.ta") === ActionType.EVENT_WENT_NOTLIVE)
        .select(
          col("payload.data.DGE_EVENT_ID").alias("event_id"),
          col("realm_id"),
          col("ts").alias("finish_at")
        )
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