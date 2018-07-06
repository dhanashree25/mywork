import org.apache.spark.sql._


class Main {
  /**
    * Spark Session
    */
  lazy val spark = {
    val s = SparkSession.builder.appName("Analytics").getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
    s.sql("set spark.sql.caseSensitive=true")
    s
  }

  /**
    * Spark Context
    */
  lazy val sc = spark.sparkContext
}