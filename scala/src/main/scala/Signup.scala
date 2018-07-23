import com.diceplatform.brain._
import com.diceplatform.brain.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.hadoop.io._

object Signup extends Main{
  def main(args: Array[String]): Unit = {
    val parser = defaultParser

    parser.head(
      """Extract-Transform-Load (ETL) task for signup table
        |
        |Parses REGISTER_USER events from and inserts into signups table
      """.stripMargin
    )

    var cli: Config = Config()
    parser.parse(args, cli) match {
      case Some(c) => cli = c
      case None => System.exit(1)
    }

    val events = spark.read.jsonSingleLine(spark, cli.path, Schema.root)

    val event_count = events.count()

//                               Table "public.signups"
//       Column    |            Type             | Collation | Nullable | Default
//    -------------+-----------------------------+-----------+----------+---------
//     customer_id | character varying(25)       |           |          |
//     realm_id    | integer                     |           |          |
//     town        | character varying(100)      |           |          |
//     country     | character (2)               |           |          |
//     ts          | timestamp without time zone |           |          |
//     device      | character varying(25)       |           |          |
//     COMPOUND SORTKEY(ts, realm_id, device, country)

    spark.sql("set spark.sql.caseSensitive=true")
    val signupdf = events.where(col("payload.data.TA") === ActionType.REGISTER_USER).join(realms, events.col("realm") === realms.col("name")).select(
              col("realm_id"),
              col("customerId").alias("customer_id"),
              col("country"),
              col("town"),
              col("ts"),
              col("payload.data.device").alias("device")
            )
    val signupdf_count=  signupdf.count()

    print("-----total------"+event_count+"-----signups------"+ signupdf_count)

    if (!cli.dryRun) {
      if (signupdf_count> 0){
          print("Writing to table")
          signupdf
            .write
            .redshift(spark)
            .mode(SaveMode.Append)
            .option("dbtable", "signups")
            .save()
      }
    } else {
      signupdf.show()
    }
 }
}
