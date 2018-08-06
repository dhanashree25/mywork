import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.diceplatform.brain.BatchSpec

class ExchangeRateCSVSpec extends BatchSpec {
  "an invalid file" should "drop invalid columns" in {
    val dateFormat = "dd MMM yyyy"

    val data = Seq(
      Row("01 January 2018", "1.1000", "1.000", " ", " ")
    )

    val schema = List(
      StructField("Date", StringType, nullable=false),
      StructField("GBP", StringType, nullable=false),
      StructField("USD", StringType, nullable=false),
      StructField("_c1", StringType, nullable=true),
      StructField("  ", StringType, nullable=true)
    )

    val sourceDF = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      StructType(schema)
    )

    val actualDF = ExchangeRateCSV.selectExchangeRates(sourceDF, dateFormat)

    val columns = actualDF.dtypes.map(dtype => dtype._1)

    assert(columns.exists(_ === "_c1") === false)
    assert(columns.exists(_ === " ") === false)
    assert(columns.length === 3)
  }


  "an valid file" should "parse date" in {
    val dateFormat = "dd MMM yyyy"

    val data = Seq(
      Row("01 January 2018", "1.0000")
    )

    val schema = List(
      StructField("Date", StringType, nullable=false),
      StructField("GBP", StringType, nullable=false)
    )

    val sourceDF = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      StructType(schema)
    )

    val actualDF = ExchangeRateCSV.selectExchangeRates(sourceDF, dateFormat)

    assert(actualDF.where(col("date") === "2018-01-01").count() === 1)
  }


  "an valid file" should "drop N/A currencies" in {
    val dateFormat = "dd MMM yyyy"

    val data = Seq(
      Row("01 January 2018", "N/A")
    )

    val schema = List(
      StructField("Date", StringType, nullable=false),
      StructField("GBP", StringType, nullable=false)
    )

    val sourceDF = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      StructType(schema)
    )

    val actualDF = ExchangeRateCSV.selectExchangeRates(sourceDF, dateFormat)

    assert(actualDF.count() === 0)
  }


  "an valid file" should "pivot currency columns to rows" in {
    val dateFormat = "dd MMM yyyy"

    val data = Seq(
      Row("01 January 2018", "1.1000", "1.0000")
    )

    val schema = List(
      StructField("Date", StringType, nullable=false),
      StructField("GBP", StringType, nullable=false),
      StructField("USD", StringType, nullable=false)
    )

    val sourceDF = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      StructType(schema)
    )

    val actualDF = ExchangeRateCSV.selectExchangeRates(sourceDF, dateFormat)

    val gbpDF = actualDF.where(col("currency") === "GBP" and col("rate") === 1.1000)
    val usdDF = actualDF.where(col("currency") === "USD" and col("rate") === 1.0000)

    assert(actualDF.count() === 2)
    assert(gbpDF.count() === 1)
    assert(usdDF.count() === 1)
  }
}
