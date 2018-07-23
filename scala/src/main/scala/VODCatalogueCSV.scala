import com.diceplatform.brain.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

case class VODCatalogueCSVConfig(path: String = "", dryRun: Boolean = false, separator: String = ";")

object VODCatalogueCSV extends Main {
  val schema = StructType(
    Array(
      StructField("vod_id", LongType, nullable=false),
      StructField("vod_dve_id", LongType, nullable=false),
      StructField("realm_operator_id", ShortType, nullable=false),
      StructField("title", StringType, nullable=false),
      StructField("description", StringType, nullable=false),
      StructField("duration", LongType, nullable=false),
      StructField("thumbnail_url", StringType, nullable=false),
      StructField("deleted", IntegerType, nullable=false),
      StructField("draft", IntegerType, nullable=false),
      StructField("imported_at", TimestampType, nullable=false),
      StructField("updated_at", TimestampType, nullable=false)
    )
  )


  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[VODCatalogueCSVConfig]("scopt") {
      head(
        """Extract-Transform-Load (ETL) task for video-on-demand (VOD) events stored in CSV

          The CSV is expected to have a header, with double quotes for the values, and semi-colon for the separator.

          If new lines are within a column value, they must be escaped as \n
          If double quotes are within a column value, they must be escaped as ""
        """.stripMargin)

      opt[String]('p', "path")
        .action((x, c) => c.copy(path = x) )
        .text("path to files, local or remote")
        .required()

      opt[Boolean]('d', "dry-run")
        .action((x, c) => c.copy(dryRun = x) )
        .text("dry run, default is false")

      opt[String]('s', "separator")
       .action((x, c) => c.copy(separator = x) )
       .text("the separator between columns, default is ;")

      opt[Boolean]('h', "header")
        .action((x, c) => c.copy(dryRun = x) )
        .text("whether to use the header (first line) as column names, default is true")
    }

    var cli: VODCatalogueCSVConfig = VODCatalogueCSVConfig()
    parser.parse(args, cli) match {
      case Some(c) => cli = c
      case None => System.exit(1)
    }

    val df = spark.read
      .option("header", value=true)
      .option("sep", value=cli.separator)
      .option("escape", value="\"")
      .schema(schema)
      .csv(cli.path)

    val updates = df
      .select(
        col("vod_id").alias("video_id"),
        col("vod_dve_id").alias("video_dve_id"),
        col("realm_operator_id").alias("realm_id"),
        col("title"),
        col("description"),
        col("duration"),
        col("thumbnail_url"),
        col("deleted").cast(BooleanType),
        col("draft").cast(BooleanType),
        lit(null).cast(StringType).alias("tags"),
        to_timestamp(lit("01-01-1970 00:00:00"), "MM-dd-yyyy HH:mm:ss").alias("imported_at"), // Override imported at
        to_timestamp(lit("01-01-1970 00:00:00"), "MM-dd-yyyy HH:mm:ss").alias("updated_at") // Override updated at
      )

    print("-----total------", df.count())

    if (!cli.dryRun) {
      updates
        .write
        .redshift(spark)
        .option("dbtable", "vod_catalogue")
        .mode(SaveMode.Append)
        .save()
    } else {
      df.show()
    }
  }
}
