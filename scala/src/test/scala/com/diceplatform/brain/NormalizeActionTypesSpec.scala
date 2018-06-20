package com.diceplatform.brain

import org.apache.spark.sql._
import org.apache.spark.sql.types._

case class Data(ta: Option[String], TA: Option[String])
case class Payload(data: Data)

class NormalizeActionTypesSpec extends BatchSpec {
  "An empty Set" should "have size 0" in {
    val someData = Seq(
      Row(Payload(Data(None, None))), // Both null
      Row(Payload(Data(Some("OK"), None))), // Right null
      Row(Payload(Data(None, Some("OK")))), // Left null
      Row(Payload(Data(Some("NOK"), Some("OK")))) // Both not-null
    )

    val schema = StructType(List(
      StructField("payload", StructType(List(
        StructField("data", StructType(List(
          StructField("ta", IntegerType, nullable=true),
          StructField("TA", StringType, nullable=true)
        )), nullable=false)
      )), nullable=false)
    ))

    val df = spark.createDataFrame(
      sc.parallelize(someData),
      schema
    )

    df.show()
  }
}

//withColumn("payload.data.ta", coalesce(col("payload.data.TA"), col("payload.data.ta"))).
//  withColumnRenamed("payload.data.ta", "payload.data.type").
//  drop(col("payload.data.TA"))


//object ActionTypes {
//  def transform(df: DataFrame): DataFrame = {
//    df.
//      withColumn("payload.data.ta", coalesce(col("payload.data.TA"), col("payload.data.ta"))).
//      withColumnRenamed("payload.data.ta", "payload.data.type").
//      drop(col("payload.data.TA"))
//  }
//}