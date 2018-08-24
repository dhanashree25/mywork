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

    val df = events.where(col("payload.data.ta").isin(ActionType.UPDATED_VOD, ActionType.NEW_VOD_FROM_DVE))
      .join(realms, col("realm") === realms.col("name"), "left_outer").cache()

    val updates = df
      .filter(col("realm_id").isNotNull)
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

    val misseddf = df.filter(col("realm_id").isNull)
    print("-----Missed VOD events------" + misseddf.count() )
    misseddf.collect.foreach(println)

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
