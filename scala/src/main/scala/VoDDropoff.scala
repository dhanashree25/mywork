import com.diceplatform.brain._
import com.diceplatform.brain.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object VoDDropoff extends Main {
  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Config]("scopt") {
      head(
        """Extract-Transform-Load (ETL) task for calculating percentage video dropoffs
          |
          |Reads from vod_plays to calculate video_duration watched and appends to the vod_dropoff table
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
    
    val df = spark.read.redshift(spark)
      .option("dbtable", "(select session_id, customer_id, realm_id, video_id, min(start_at) start_time, max(end_at) end_time, max(duration) progress from vod_play group by session_id, customer_id, realm_id, video_id)") 
      .load()

    val vod_catalogue = spark
      .read
      .redshift(spark)
      .option("dbtable", "(select video_dve_id,max(duration) duration from vod_catalogue group by video_dve_id)")
      .load()

//                              Table "public.vod_dropoff"
//         Column     |            Type             | Collation | Nullable | Default 
//    ----------------+-----------------------------+-----------+----------+---------
//     realm_id       | integer                     |           |          | 
//     video_id       | integer                     |           |          | 
//     customer_id    | character varying(32)       |           | not null | 
//     year           | integer                     |           |          | 
//     month          | integer                     |           |          | 
//     day            | integer                     |           |          | 
//     dow            | integer                     |           |          | 
//     hour           | integer                     |           |          | 
//     start_at       | timestamp without time zone |           |          | 
//     video_duration | integer                     |           | not null | 
//     progress       | integer                     |           | not null | 
//     perc_drop      | double precision            |           |          | 

//     COMPOUND SORTKEY(realm_id, video_id, customer_id);
    val updates = df.
        join(vod_catalogue, df.col("video_id") === vod_catalogue.col("video_dve_id")) 
        .select(
            df.col("realm_id"),
            df.col("video_id"),
            df.col("customer_id"),
            df.col("start_time").alias("start_at"),
            year(col("start_time")).alias("year"),
            weekofyear(col("start_time")).alias("dow"),
            month(col("start_time")).alias("month"),
            dayofmonth(col("start_time")).alias("day"),
            hour(col("start_time")).alias("hour"),
            col("progress"),
            col("duration"),
            (col("progress")*100/col("duration") ).alias("perc_drop"))
        .where(col("duration")=!= 0)
       
        if (!cli.dryRun) {
              print("Writing to table") 
              
          updates.write
            .redshift(spark)
            .option("dbtable", "vod_dropoff")
            .mode(SaveMode.Overwrite)
            .save()
            print("done")
        } else {
          updates.show()
    }
  }
}
