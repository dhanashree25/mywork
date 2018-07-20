import org.apache.spark._
import org.apache.spark.sql._
import com.diceplatform.brain.implicits._


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
}
