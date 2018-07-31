import com.diceplatform.brain.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

case class RollupConfig(path: String = "", dryRun: Boolean = false, from: String = "", to: String = "", window: String = "1 hour")
object RevenueRollup extends Main{
  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[RollupConfig]("scopt") {
      head(
        """Extract-Transform-Load (ETL) task for revenue_* roll-up table
        """.stripMargin
      )

      opt[Boolean]('d', "dry-run")
        .action((x, c) => c.copy(dryRun = x) )
        .optional()
        .text("dry run")

      opt[String]('w', "window")
       .required()
       .action((x, c) => c.copy(window = x) )
       .text("aggregate the records by a window")

      opt[String]('f', "from")
        .required()
        .action((x, c) => c.copy(from = x) )
        .text("the date range to select from, inclusive. Formatted at yyyy-MM-dd HH:mm:ss")

       opt[String]('t', "to")
        .required()
        .action((x, c) => c.copy(to = x) )
        .text("the date range to select to, exclusive. Formatted at yyyy-MM-dd HH:mm:ss")
    }

    var cli: RollupConfig = RollupConfig()
    parser.parse(args, cli) match {
      case Some(c) => cli = c
      case None => System.exit(1)
    }

    val payments = spark
      .read
      .redshift(spark)
      .option("dbtable", "payment")
      .load()
      .where(col("ts") < cli.to)
      .where(col("ts") >= cli.from)

    val revenue = payments
      .na.fill(0, Seq("amount_with_tax"))
      .where(col("amount_with_tax") =!= 0.0)
      .cube(
        col("realm_id"),
        col("payment_provider"),
        col("currency"),
        window(col("ts"), cli.window).alias("ts")
      )
      .agg(
//        count(col("realm_id")).alias("count"),
        sum(col("amount_with_tax")).alias("amount")
      )
      .na.drop()
      .select(
        col("realm_id"),
        col("payment_provider"),
        col("currency"),
        col("amount"),
        col("amount").alias("amount_usd"),
//        col("countdd,
        col("ts").getItem("start").alias("ts")
      )
      .sort("ts")

    if (!cli.dryRun) {
      val dbtable = cli.window.replaceAll("^[0-9]\\s+", "") match {
        case "day" => "daily"
        case period => period + "ly"
      }

      revenue
        .write
        .redshift(spark)
        .option("dbtable", "revenue_" + dbtable)
        .mode(SaveMode.Append)
        .save()
    } else {
      revenue.show()
    }
 }
}
