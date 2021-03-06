import com.diceplatform.brain.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

case class RealmCSVConfig(path: String = "", dryRun: Boolean = false, separator: String = ",", header:Boolean = true)

object RealmCSV extends Main {
  val schema = StructType(
    Array(
      StructField("realm_id", IntegerType, nullable=false),
      StructField("name", StringType, nullable=false)
    )
  )


  def main(args: Array[String]): Unit = {
    val parser = csvParser

    parser.head("Extract-Transform-Load (ETL) task for realms stored in CSV")

    var cli: CSVConfig = CSVConfig()
    parser.parse(args, cli) match {
      case Some(c) => cli = c
      case None => System.exit(1)
    }

    val realms = spark.read
      .option("header", value=cli.header)
      .option("sep", value=cli.separator)
      .option("escape", value="\"")
      .schema(schema)
      .csv(cli.path)

    print("-----total------", realms.count())

    if (!cli.dryRun) {
      realms
        .write
        .redshift(spark)
        .option("dbtable", "realm")
        .mode(SaveMode.Append)
        .save()
    } else {
      realms.show()
    }
  }
}
