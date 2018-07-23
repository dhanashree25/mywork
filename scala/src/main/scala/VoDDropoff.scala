import com.diceplatform.brain._
import com.diceplatform.brain.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object VoDDropoff extends Main {
  def main(args: Array[String]): Unit = {
     val parser = defaultParser
     
     parser.head(
        """Extract-Transform-Load (ETL) task for calculating percentage video dropoffs
          |
          |Reads from vod_plays to calculate video_duration watched and appends to the vod_dropoff table
        """.stripMargin
    )

    var cli: Config = Config()
    parser.parse(args, cli) match {
      case Some(c) => cli = c
      case None => System.exit(1)
    }
     
    val vod_play = spark.read.redshift(spark)
      .option("dbtable", "(select session_id, customer_id, realm_id, video_id, min(start_at) start_at, max(end_at) end_at, max(duration) progress from vod_play group by session_id, customer_id, realm_id, video_id)") 
      .load()

    val vod_catalogue = spark
      .read
      .redshift(spark)
      .option("dbtable", "(select video_dve_id,max(duration) duration from vod_catalogue where duration >0  group by video_dve_id)")
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
    val updates = vod_play.
        join(vod_catalogue, vod_play.col("video_id") === vod_catalogue.col("video_dve_id")) 
        .select(
            vod_play.col("realm_id"),
            vod_play.col("video_id"),
            vod_play.col("customer_id"),
            vod_play.col("start_at"),
            year(col("start_at")).alias("year"),
            weekofyear(col("start_at")).alias("dow"),
            month(col("start_at")).alias("month"),
            dayofmonth(col("start_at")).alias("day"),
            hour(col("start_at")).alias("hour"),
            col("progress"),
            col("duration"),
            (col("progress")*100/col("duration") ).alias("perc_drop"))
       
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
