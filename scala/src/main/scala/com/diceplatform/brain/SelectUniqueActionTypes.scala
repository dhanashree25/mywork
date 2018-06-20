package com.diceplatform.brain

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object SelectUniqueActionTypes {
  def select(df: DataFrame): DataFrame = {
    df.
      select(col("payload.action"), col("payload.data.ta")).
      distinct()
  }

}
