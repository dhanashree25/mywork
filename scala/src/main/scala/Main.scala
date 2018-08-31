import org.apache.spark._
import org.apache.spark.sql._
import com.diceplatform.brain.implicits._

case class Config(path: String = "", dryRun: Boolean = false)
case class CSVConfig(path: String = "", dryRun: Boolean = false, separator: String = ",", header:Boolean = true)
case class DateBucketConfig(path: String = "", dryRun: Boolean = false, dateBucket: String = "")

class Main {
  /**
    * Spark Session
    */
  lazy val spark: SparkSession = {
    val s = SparkSession.builder.appName("Analytics").getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
    s.sql("set spark.sql.caseSensitive=true")
    s
  }

  /**
    * Spark Context
    */
  lazy val sc: SparkContext = spark.sparkContext

  lazy val sqlc: SQLContext = spark.sqlContext

  /**
    * Realms table
    *
    *                  Table "public.realm"
    *   Column  |          Type          | Collation | Nullable | Default
    *  ---------+------------------------+-----------+----------+---------
    *  realm_id | integer                |           | not null |
    *  name     | character varying(256) |           | not null |
    */
  lazy val realms: DataFrame = {
    spark
      .read
      .redshift(spark)
      .option("dbtable", "realm")
      .load()
  }

  lazy val defaultParser: scopt.OptionParser[Config] = {
    new scopt.OptionParser[Config]("scopt") {
      opt[String]("path")
        .action((x, c) => c.copy(path = x) )
        .text("path to files, local or remote")
        .required()

      opt[Boolean]("dry-run")
        .action((x, c) => c.copy(dryRun = x) )
        .optional()
        .text("dry run")
    }
  }

  lazy val csvParser: scopt.OptionParser[CSVConfig] = {
    new scopt.OptionParser[CSVConfig]("scopt") {
      opt[String]("path")
        .action((x, c) => c.copy(path = x) )
        .text("path to files, local or remote")
        .required()

      opt[Boolean]("dry-run")
        .action((x, c) => c.copy(dryRun = x) )
        .text("dry run, default is false")

      opt[String]("separator")
        .action((x, c) => c.copy(separator = x) )
        .text("the separator between columns, default is ,")

      opt[Boolean]("header")
        .action((x, c) => c.copy(header = x) )
        .text("whether to use the header (first line) as column names, default is true")
    }
  }

  lazy val bucketParser: scopt.OptionParser[DateBucketConfig] = {
    new scopt.OptionParser[DateBucketConfig]("scopt") {
      opt[String]("path")
        .action((x, c) => c.copy(path = x) )
        .text("path to files, local or remote")
        .required()

      opt[Boolean]("dry-run")
        .action((x, c) => c.copy(dryRun = x) )
        .optional()
        .text("dry run")

      opt[String]("date-bucket")
        .action((x, c) => c.copy(dateBucket = x) )
        .text("path to date bucket, required only for EventDateBucket job")
        .optional()
    }
  }
}
