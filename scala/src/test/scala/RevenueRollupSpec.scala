import com.diceplatform.brain.BatchSpec

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.sql.{Date, Timestamp}

class RevenueRollupSpec extends BatchSpec {
  val data = Seq(
    Row(1, "STRIPE", "TRY", 0, Timestamp.valueOf("2018-01-01 00:00:00")), // dropped
    Row(1, "STRIPE", "AUD", null, Timestamp.valueOf("2018-01-01 00:00:00")), // dropped
    Row(1, "STRIPE", "USD", 100, Timestamp.valueOf("2018-01-01 00:00:00")), // row 1
    Row(1, "STRIPE", "USD", 100, Timestamp.valueOf("2018-01-01 00:01:00")), // row 1
    Row(1, "STRIPE", "GBP", 799, Timestamp.valueOf("2018-01-01 00:02:00")), // row 2 (rollup currency)
    Row(1, "STRIPE", "GBP", 799, Timestamp.valueOf("2018-01-01 00:03:00")), // row 2 (rollup by currency)
    Row(1, "GOOGLE_IAP", "USD", 100, Timestamp.valueOf("2018-01-01 00:04:00")), // row 3 (rollup by payment provider)
    Row(1, "GOOGLE_IAP", "USD", 100, Timestamp.valueOf("2018-01-01 00:05:00")), // row 3 (rollup by payment provider)
    Row(2, "APPLE_IAP", "USD", 100, Timestamp.valueOf("2018-01-01 00:06:00")), // row 4 (rollup by realm)
    Row(2, "APPLE_IAP", "USD", 100, Timestamp.valueOf("2018-01-01 00:07:00")), // row 4 (rollup by realm)
    Row(1, "STRIPE", "USD", 100, Timestamp.valueOf("2018-01-01 01:00:00")), // row 5 (rollup by window)
    Row(1, "STRIPE", "USD", 100, Timestamp.valueOf("2018-01-01 01:01:00")) // row 5 (rollup by window)
  )

  val schema = List(
    StructField("realm_id", IntegerType, nullable=false),
    StructField("payment_provider", StringType, nullable=false),
    StructField("currency", StringType, nullable=false),
    StructField("amount_with_tax", IntegerType, nullable=true), // TODO: Use double type?
    StructField("ts", TimestampType, nullable=false)
  )

  "givena payments" should "filter from" in {
    val from = "2018-01-01 00:00:00"

    val sourceDF = spark.createDataFrame(
      sc.parallelize(data),
      StructType(schema)
    )

    val actualDF = RevenueRollup.selectPayments(sourceDF, from)

    assert(actualDF.count() === 12)
  }

  "given payments" should "filter from inclusive and to" in {
    val from = "2018-01-01 00:00:00"
    val to = "2018-01-01 01:00:00"

    val sourceDF = spark.createDataFrame(
      sc.parallelize(data),
      StructType(schema)
    )

    val actualDF = RevenueRollup.selectPayments(sourceDF, from, to)

    assert(actualDF.count() === 10)
    assert(actualDF.where(col("ts") === "2018-01-01 01:00:00").count() === 0)
    assert(actualDF.where(col("ts") === "2018-01-01 01:01:00").count() === 0)
  }


  "given payments" should "drop amounts with null or zero" in {
    val sourceDF = spark.createDataFrame(
      sc.parallelize(data),
      StructType(schema)
    )

    val actualDF = RevenueRollup.rollupRevenue(sourceDF, "hour")

    assert(actualDF.where(col("currency") === "TRY").count() === 0)
    assert(actualDF.where(col("currency") === "AUD").count() === 0)
  }


  "given payments" should "generate rollup of revenue by realm" in {
    val sourceDF = spark.createDataFrame(
      sc.parallelize(data),
      StructType(schema)
    )

    val actualDF = RevenueRollup.rollupRevenue(sourceDF, "hour")

    val byRealm1 = actualDF.where(col("realm_id") === 1)
    val byRealm2 = actualDF.where(col("realm_id") === 2)

    assert(actualDF.groupBy(col("realm_id")).sum().count() === 2)
    assert(byRealm1.count() === 4)
    assert(byRealm2.count() === 1)
  }


  "given payments" should "generate rollup of revenue by payment provider" in {
    val sourceDF = spark.createDataFrame(
      sc.parallelize(data),
      StructType(schema)
    )

    val actualDF = RevenueRollup.rollupRevenue(sourceDF, "hour")

    assert(actualDF.groupBy(col("payment_provider"), col("ts")).sum().count() === 4)
    assert(actualDF.where(col("payment_provider") === "STRIPE").count() === 3)
    assert(actualDF.where(col("payment_provider") === "GOOGLE_IAP").count() === 1)
    assert(actualDF.where(col("payment_provider") === "APPLE_IAP").count() === 1)
  }


  "given payments" should "generate rollup of revenue" in {
    val sourceDF = spark.createDataFrame(
      sc.parallelize(data),
      StructType(schema)
    )

    val actualDF = RevenueRollup.rollupRevenue(sourceDF, "hour")

    val row1 = actualDF.where(
      col("realm_id") === 1 and
      col("payment_provider") === "STRIPE" and
      col("currency") === "USD" and
      col("ts") === "2018-01-01 00:00:00" and
      col("amount") === 200
    )

    val row2 = actualDF.where(
      col("realm_id") === 1 and
      col("payment_provider") === "STRIPE" and
      col("currency") === "GBP" and
      col("ts") === "2018-01-01 00:00:00" and
      col("amount") === 1598
    )

    val row3 = actualDF.where(
      col("realm_id") === 1 and
      col("payment_provider") === "GOOGLE_IAP" and
      col("currency") === "USD" and
      col("ts") === "2018-01-01 00:00:00" and
      col("amount") === 200
    )

    val row4 = actualDF.where(
      col("realm_id") === 2 and
      col("payment_provider") === "APPLE_IAP" and
      col("currency") === "USD" and
      col("ts") === "2018-01-01 00:00:00" and
      col("amount") === 200
    )

    val row5 = actualDF.where(
      col("realm_id") === 1 and
      col("payment_provider") === "STRIPE" and
      col("currency") === "USD" and
      col("ts") === "2018-01-01 01:00:00" and
      col("amount") === 200
    )

    assert(actualDF.count() === 5)
    assert(row1.count() === 1)
    assert(row2.count() === 1)
    assert(row3.count() === 1)
    assert(row4.count() === 1)
    assert(row5.count() === 1)
  }


  "given a dataframe" should "add column in currency" in {
    // We process the data after the whole day, so Monday we can use the existing exchange rate
    val data = Seq(
//      Row(1, "STRIPE", "AUD", 10000, Timestamp.valueOf("2018-01-01 00:00:00")), // No exchange rate
//      Row(1, "STRIPE", "GBP", 10000, Timestamp.valueOf("2018-01-02 00:00:00")), // No exchange rate
      Row(1, "STRIPE", "GBP", 10000, Timestamp.valueOf("2018-01-01 00:00:00")), // Monday (should use Monday rate)
      Row(1, "STRIPE", "GBP", 20000, Timestamp.valueOf("2018-01-06 00:00:00")), // Saturday (should use Friday rate)
      Row(1, "STRIPE", "GBP", 30000, Timestamp.valueOf("2018-01-07 00:00:00")) // Sunday (should use Friday rate)
      // TODO: Add null rate
    )

    val exchangeRateDF = spark.createDataFrame(
      sc.parallelize(
        Seq(
          Row(Date.valueOf("2018-01-01"), "GBP", 1.15), // Monday rate
          Row(Date.valueOf("2018-01-01"), "USD", 1.30),
          Row(Date.valueOf("2018-01-05"), "GBP", 1.10), // Friday rate
          Row(Date.valueOf("2018-01-05"), "USD", 1.20)
        )
      ),
      StructType(
        List(
          StructField("date", DateType, nullable=false),
          StructField("currency", StringType, nullable=false),
          StructField("rate", DoubleType, nullable=false)
        )
      )
    )

    // 10000/1.15*1.30

    val sourceDF = spark.createDataFrame(
      sc.parallelize(data),
      StructType(schema)
    )

    val actualDF = RevenueRollup.withColumnInCurrency(sourceDF, exchangeRateDF)

    val columns = actualDF.dtypes.map(dtype => dtype._1)

    assert(columns.length === 6)
    assert(columns.exists(_.equals("amount_with_tax_usd")) === true)

    // 10000 GBP / 1.15 EUR rate = 8,695.652173913 EUR
    // 8,695.652173913 EUR * 1.30 USD rate = 11,304.3478260869 USD
    // round(11,304.3478260869) = 11.304
    assert(actualDF.where(
      col("currency") === "GBP" and
      col("ts") === "2018-01-01 00:00:00" and
      col("amount_with_tax_usd") === 11304
    ).count() === 1)

    // 20000 GBP / 1.10 EUR rate = 18,181.8181818182 EUR
    // 18,181.8181818182 EUR * 1.20 USD rate = 21,818.1818181818 USD
    // round(21,818.1818181818) = 21,818
    assert(actualDF.where(
      col("currency") === "GBP" and
        col("ts") === "2018-01-06 00:00:00" and
        col("amount_with_tax_usd") === 21818
    ).count() === 1)

    // 30000 GBP / 1.10 EUR rate = 27,272.7272727273 EUR
    // 27,272.7272727273 EUR * 1.20 USD rate = 32,727.2727272728 USD
    // round(32,727.2727272728) = 32,727
    assert(actualDF.where(
      col("currency") === "GBP" and
        col("ts") === "2018-01-07 00:00:00" and
        col("amount_with_tax_usd") === 32727
    ).count() === 1)
  }
}
