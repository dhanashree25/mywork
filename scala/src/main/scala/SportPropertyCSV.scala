import com.diceplatform.brain.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object SportPropertyCSV extends Main {
  val schema = StructType(
    Array(
      StructField("sport_id", IntegerType, nullable=false),
      StructField("property_id", IntegerType, nullable=false),
      StructField("name", StringType, nullable=false)
    )
  )


  def main(args: Array[String]): Unit = {
    val parser = csvParser

    parser.head("Extract-Transform-Load (ETL) task for properties stored in CSV")

    var cli: CSVConfig = CSVConfig()
    parser.parse(args, cli) match {
      case Some(c) => cli = c
      case None => System.exit(1)
    }

    val sport_properties = spark.read
      .option("header", value=cli.header)
      .option("sep", value=cli.separator)
      .option("escape", value="\"")
      .schema(schema)
      .csv(cli.path)

    print("-----total------", sport_properties.count())

    if (!cli.dryRun) {
      sport_properties
        .write
        .redshift(spark)
        .option("dbtable", "sport_property")
        .mode(SaveMode.Append)
        .save()
    } else {
      sport_properties.show()
    }
  }
}
