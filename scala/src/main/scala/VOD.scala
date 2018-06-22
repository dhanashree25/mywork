import com.diceplatform.brain._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._
import java.util.Scanner

case class Config(path: String = "")

object VOD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Analytics").getOrCreate()

    val parser = new scopt.OptionParser[Config]("scopt") {
      head(
        """Extract-Transform-Load (ETL) task for catalogue table
          |
          |Parses UPDATED_VOD and NEW_VOD_FROM_DVE events from and creates catalogue table
        """.stripMargin)

      opt[String]('p', "path").action( (x, c) =>
        c.copy(path = x) ).text("path to files")
    }

    var path: String = ""
    parser.parse(args, Config()) match {
      case Some(c) => path = c.path
      case None => System.exit(1)
    }

    // Parse single-line multi-JSON object into single-line single JSON object
    // TODO: Refactor, _.replace() returns entire string in memory
    // TODO: Refactor: }{ is very naive, if it occurs in a string, it will break
    val rdd = spark.sparkContext
      .textFile(path)
      .map(_.replace("}{", "}\n{"))
      .flatMap(i => new Scanner(i).useDelimiter("\n").asScala.toStream)

    val ds = spark.createDataset(rdd)(Encoders.STRING)

    spark.sql("set spark.sql.caseSensitive=true")

    val df = spark.read
      .option("allowSingleQuotes", false)
      .option("multiLine", false)
      .schema(Schema.root)
      .json(ds)
      .where(col("payload.data.ta").isin(ActionType.UPDATED_VOD, ActionType.NEW_VOD_FROM_DVE))


    //
    //  Table "public.realm"
    //   Column  |          Type          | Collation | Nullable | Default
    //  ---------+------------------------+-----------+----------+---------
    //  realm_id | integer                |           | not null |
    //  name     | character varying(256) |           | not null |
    //

    val realms = spark
      .read
      .format("jdbc")
      .option("driver", spark.conf.get("spark.jdbc.driver", "com.amazon.redshift.jdbc.Driver"))
      .option("url", spark.conf.get("spark.jdbc.url"))
      .option("user", spark.conf.get("spark.jdbc.username"))
      .option("password", spark.conf.get("spark.jdbc.password"))
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


    val mkString = udf((x:Seq[String]) => x.toSet.mkString(","))

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
        mkString(col("payload.data.v.tags")).alias("tags"),
        when(col("payload.data.ta") === ActionType.NEW_VOD_FROM_DVE, col("ts"))
          .otherwise(lit(null))
          .alias("imported_at"),
        col("ts").alias("updated_at")
      )


    updates
      .write
      .format("jdbc")
      .mode(SaveMode.Append)
      .option("driver", spark.conf.get("spark.jdbc.driver", "com.amazon.redshift.jdbc.Driver"))
      .option("url", spark.conf.get("spark.jdbc.url"))
      .option("user", spark.conf.get("spark.jdbc.username"))
      .option("password", spark.conf.get("spark.jdbc.password"))
      .option("dbtable", "catalogue")
      .save()
  }
}