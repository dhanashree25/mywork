import com.diceplatform.brain._
import com.diceplatform.brain.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object LiveCatalogue extends Main {
  def main(args: Array[String]): Unit = {
    val parser = defaultParser

    parser.head(
      """Extract-Transform-Load (ETL) task for live stream events
        |
        |Parses LIVESTREAMING_EVENT_UPDATED events from JSON objects stored in files and appends to the live_stream table
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

    val df = events.where(col("payload.data.ta") === ActionType.LIVESTREAMING_EVENT_UPDATED)
      .join(realms, col("realm") === realms.col("name"), "left_outer").cache()

    val updates = df.filter(col("realm_id").isNotNull)
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

    print("-----total------",events_count,"-----live------", updates_count)

    val misseddf = df
      .filter(col("realm_id").isNull)
    print("-----Missed Live events------" + misseddf.count() )
    misseddf.collect.foreach(println)

    if (!cli.dryRun) {
      updates
        .write
        .redshift(spark)
        .mode(SaveMode.Append)
        .option("dbtable", "live_catalogue")
        .save()
    } else {
      updates.show()
    }
  }
}
