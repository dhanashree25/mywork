package com.diceplatform.brain

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{coalesce, col}

object NormalizeActionTypes {
  def transform(df: DataFrame): DataFrame = {
    df.
      withColumn("payload.data.ta", coalesce(col("payload.data.TA"), col("payload.data.ta"))).
      withColumnRenamed("payload.data.ta", "payload.data.type").
      drop(col("payload.data.TA"))
  }
}