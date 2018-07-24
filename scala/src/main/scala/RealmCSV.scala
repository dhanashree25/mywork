import com.diceplatform.brain.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

case class RealmCSVConfig(path: String = "", dryRun: Boolean = false, separator: String = ",", header:Boolean = true)

object RealmCSV extends Main {
  val schema = StructType(
    Array(
      StructField("realm_id", StringType, nullable=false),
      StructField("name", StringType, nullable=false)
    )
  )


  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[RealmCSVConfig]("scopt") {
      head("Extract-Transform-Load (ETL) task for realms stored in CSV")

      opt[String]('p', "path")
        .action((x, c) => c.copy(path = x) )
        .text("path to files, local or remote")
        .required()

      opt[Boolean]('d', "dry-run")
        .action((x, c) => c.copy(dryRun = x) )
        .text("dry run, default is false")

      opt[String]('s', "separator")
       .action((x, c) => c.copy(separator = x) )
       .text("the separator between columns, default is ,")

      opt[Boolean]('h', "header")
        .action((x, c) => c.copy(header = x) )
        .text("whether to use the header (first line) as column names, default is true")
    }

    var cli: RealmCSVConfig = RealmCSVConfig()
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
        .mode(SaveMode.Overwrite)
        .save()
    } else {
      realms.show()
    }
  }
}
