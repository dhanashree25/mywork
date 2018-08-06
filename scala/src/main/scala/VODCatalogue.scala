import com.diceplatform.brain._
import com.diceplatform.brain.implicits._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object VODCatalogue extends Main {
  def main(args: Array[String]): Unit = {
    val parser = defaultParser

    parser.head(
      """Extract-Transform-Load (ETL) task for video-on-demand (VOD) events
        |
        |Parses UPDATED_VOD and NEW_VOD_FROM_DVE events from JSON objects stored in files and appends to the catalogue table
      """.stripMargin
    )

    var cli: Config = Config()
    parser.parse(args, cli) match {
      case Some(c) => cli = c
      case None => System.exit(1)
    }

    val events = spark.read
        .jsonSingleLine(spark, cli.path, Schema.root)

    val events_count = events.count()
    // TODO: Add support for stream events

   //
   //                             Table "public.catalogue"
   //    Column     |            Type             | Collation | Nullable | Default
   // --------------+-----------------------------+-----------+----------+---------
   // video_id      | integer                     |           |          |
   // video_dve_id  | integer                     |           |          |
   // realm_id      | integer                     |           |          |
   // title         | character varying(1024)     |           |          |
   // description   | character varying(1024)     |           |          |
   // duration      | integer                     |           |          |
   // thumbnail_url | character varying(1024)     |           |          |
   // deleted       | boolean                     |           |          |
   // draft         | boolean                     |           |          |
   // tags          | character varying(1024)     |           |          |
   // imported_at   | timestamp without time zone |           |          |
   // updated_at    | timestamp without time zone |           |          |
   //
    val df = events.where(col("payload.data.ta").isin(ActionType.UPDATED_VOD, ActionType.NEW_VOD_FROM_DVE))

    val updates = df
      .join(realms, df.col("realm") === realms.col("name"))
      .select(
        col("payload.data.vid").alias("video_id"),
        col("payload.data.v.vodDveId").alias("video_dve_id"),
        col("realm_id"),
        col("payload.data.v.title"),
        col("payload.data.v.description"),
        col("payload.data.v.duration"),
        col("payload.data.v.thumbnailUrl").alias("thumbnail_url"),
        col("payload.data.v.deleted"),
        col("payload.data.v.draft"),
        UDF.mkString(col("payload.data.v.tags")).alias("tags"),
        when(col("payload.data.ta") === ActionType.NEW_VOD_FROM_DVE, col("ts"))
          .otherwise(lit(null))
          .alias("imported_at"),
        col("ts").alias("updated_at")
      )

    val updates_count = updates.count()

    print("-----total------",events_count,"-----vod------", updates_count)

    if (!cli.dryRun) {
          updates
            .write
            .redshift(spark)
            .option("dbtable", "vod_catalogue")
            .mode(SaveMode.Append)
            .save()

    } else {
      updates.show()
    }
  }
}
