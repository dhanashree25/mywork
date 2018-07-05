import com.diceplatform.brain._
import com.diceplatform.brain.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.hadoop.io._

object LiveStream extends Main {
  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Config]("scopt") {
      head(
        """Extract-Transform-Load (ETL) task for live stream events
          |
          |Parses LIVESTREAMING_EVENT_UPDATED events from JSON objects stored in files and appends to the live_stream table
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
    //                                 Table "public.live_stream"
    //           Column           |            Type             | Collation | Nullable | Default
    // ---------------------------+-----------------------------+-----------+----------+---------
    //  realm_id                  | integer                     |           | not null |
    //  title                     | character varying(1024)     |           | not null |
    //  event_id                  | integer                     |           | not null |
    //  sport_id                  | integer                     |           | not null |
    //  tournament_id             | integer                     |           |          |
    //  property_id               | integer                     |           |          |
    //  duration                  | integer                     |           | not null |
    //  thumbnail_url             | character varying(1024)     |           | not null |
    //  deleted                   | boolean                     |           | not null |
    //  geo_restriction_type      | character(9)                |           | not null |
    //  geo_restriction_countries | character varying(2048)     |           | not null |
    //  start_at                  | timestamp without time zone |           | not null |
    //  finish_at                 | timestamp without time zone |           | not null |
    //  updated_at                | timestamp without time zone |           | not null |
    //
    val df = events.where(col("payload.data.ta") === ActionType.LIVESTREAMING_EVENT_UPDATED)
    val updates = df
      .join(realms, df.col("realm") === realms.col("name"))
      .select(
        col("realm_id"),
        col("payload.data.v.title"),
        col("payload.data.v.eventId").alias("event_id"),
        col("payload.data.v.sportId").alias("sport_id"),
        col("payload.data.v.tournamentId").alias("tournament_id"),
        col("payload.data.v.propertyId").alias("property_id"),
        (unix_timestamp(col("payload.data.v.endDate")) - unix_timestamp(col("payload.data.v.startDate"))).alias("duration"),
        col("payload.data.v.thumbnail").alias("thumbnail_url"),
        col("payload.data.v.deleted"),
        col("payload.data.v.geoRestriction.geoRestrictionType").alias("geo_restriction_type"), // TODO: Normalize?
        UDF.mkString(col("payload.data.v.geoRestriction.countries")).alias("geo_restriction_countries"),
        col("payload.data.v.startDate").alias("start_at"),
        col("payload.data.v.endDate").alias("finish_at"),
        col("payload.data.v.updatedAt").alias("updated_at") // What about "ts"?
      )
     
    val updates_count=  updates.count()
    print("-----total------",events_count,"-----vod------", updates_count)

    if (!cli.dryRun) {
      updates
        .write
        .redshift(spark)
        .mode(SaveMode.Append)
        .option("dbtable", "live_stream")
        .save()
    } else {
      updates.show()
    }
  }
}