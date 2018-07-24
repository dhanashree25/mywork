import com.diceplatform.brain.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object SportPropertyTournamentCSV extends Main {
  val schema = StructType(
    Array(
      StructField("property_id", IntegerType, nullable=false),
      StructField("tournament_id", IntegerType, nullable=false),
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

    val properties = spark.read
      .redshift(spark)
      .option("dbtable", "sport_property")
      .load()

    val csv = spark.read
      .option("header", value=cli.header)
      .option("sep", value=cli.separator)
      .option("escape", value="\"")
      .schema(schema)
      .csv(cli.path)

    val sport_property_tournaments = csv
      .join(properties, properties.col("property_id") === csv.col("property_id"))
      .select(
        properties.col("sport_id"),
        csv.col("property_id"),
        csv.col("tournament_id"),
        csv.col("name")
      )

    print("-----total------", sport_property_tournaments.count())

    if (!cli.dryRun) {
      sport_property_tournaments
        .write
        .redshift(spark)
        .option("dbtable", "sport_property_tournament")
        .mode(SaveMode.Append)
        .save()
    } else {
      sport_property_tournaments.show()
    }
  }
}
