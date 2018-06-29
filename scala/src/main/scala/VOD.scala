import com.diceplatform.brain._
import com.diceplatform.brain.implicits._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.hadoop.io._

case class Config(path: String = "", dryRun: Boolean = false)

object VOD extends Main {
  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Config]("scopt") {
      head(
        """Extract-Transform-Load (ETL) task for video-on-demand (VOD) events
          |
          |Parses UPDATED_VOD and NEW_VOD_FROM_DVE events from JSON objects stored in files and appends to the catalogue table
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

    val events = spark.read
        .jsonSingleLine(spark, cli.path, Schema.root)

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

    if (!cli.dryRun) {
          updates
            .write
            .redshift(spark)
            .option("dbtable", "catalogue")
            .mode(SaveMode.Append)
            .save()
    } else {
      updates.show()
    }
  }
}