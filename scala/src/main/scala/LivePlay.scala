import com.diceplatform.brain._
import com.diceplatform.brain.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object LivePlay extends Main {
  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Config]("scopt") {
      head(
        """Extract-Transform-Load (ETL) task for calculating Live video play
          |
          |Parses LIVE_ACTION from JSON objects stored in files and appends to the live_play table
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

    val events = spark.read.jsonSingleLine(spark, cli.path, Schema.root)
    
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
      
    val df = events.where(col("payload.action") === Action.LIVE_WATCHING)
    
    val newSessions = events
      .where(col("payload.action") === Action.VOD_PROGRESS)
      .select(collect_set(col("payload.cid")).as("session_ids"))
      .first()
      .getList[String](0)
      .toArray()        //This will need optimisation

    val previousPlays = spark.read.redshift(spark)
      .option("dbtable", "live_play")
      .load()
      .where(col("session_id").isin(newSessions:_*)) // this will need to be optimised
    
    val live_updates = df
      .join(realms, df.col("realm") === realms.col("name"))
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

      val reorderedColumnNames: Array[String] = Array("realm_id",
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
                                                 
       val updates = live_updates.select(reorderedColumnNames.head, reorderedColumnNames.tail: _*).union(previousPlays)
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
      
    
    if (!cli.dryRun) {
          print("Writing to table") 
           updates
            .write
            .redshift(spark)
            .option("dbtable", "live_play")
            .mode(SaveMode.Append)
            .save()
    } else {
      updates.show()
    }
  }
}