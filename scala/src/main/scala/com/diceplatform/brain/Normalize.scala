package com.diceplatform.brain

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.diceplatform.brain.UDF._
import com.diceplatform.brain.implicits._

object Normalize {
  def transform(df: DataFrame): DataFrame = {
    val spark = df.sparkSession
    // TODO: Consider resetting / setting it globally
    spark.sql("set spark.sql.caseSensitive=true")

    df
      // Combine variants of "ta"
      .withColumn("ta", coalesce(col("payload.data.TA"), col("payload.data.ta")))

      // Drop legacy columns
      .dropNested("payload.data.ta")
      .dropNested("payload.data.TA")

      // Convert action integer to string
      .withColumn("action", actionIntToString(col("payload.action")))

      // Combine "action" and "ta"
      .withColumn("action", when(col("ta") === null, col("action"))
        .otherwise(
          when(col("ta") isin ("OK", "NOK"), concat(col("action"), lit("_"), col("ta")))
            .otherwise(col("ta"))
        )
      )

      // Drop intermediate columns
      .drop("ta")

      // Drop legacy columns
      .dropNested("payload.action")
  }
}


