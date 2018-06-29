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
    val parser = new scopt.OptionParser[Config]("scopt") {
      head(
        """Extract-Transform-Load (ETL) task for signup table
          |
          |Parses REGISTER_USER events from and inserts into signups table
        """.stripMargin)

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
    
    val realms = spark
      .read
      .redshift(spark)
      .option("dbtable", "realm")
      .load()
        
//                               Table "public.signups"
//       Column    |            Type             | Collation | Nullable | Default 
//    -------------+-----------------------------+-----------+----------+---------
//     customer_id | character varying(25)       |           |          | 
//     realm       | character varying(50)       |           |          | 
//     town        | character varying(100)      |           |          | 
//     country     | character varying(100)      |           |          | 
//     ts          | timestamp without time zone |           |          | 
//     device      | character varying(25)       |           |          | 
//     COMPOUND SORTKEY(ts, realm, device, country)

    
    spark.sql("set spark.sql.caseSensitive=true")
    val signupdf = events.where(col("payload.data.TA") === ActionType.REGISTER_USER)
            .select(
              expr("realm"),
              col("customerId").alias("customer_id"),
              col("country"),
              col("town"),
              col("ts"),
              expr("payload.data.device as device")
            )
            
    print("-----total------"+events.count()+"-----signups------"+ signupdf.count())
    
    if (!cli.dryRun) {
      if (signupdf.count()> 0){
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