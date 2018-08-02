package com.diceplatform.brain

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec}

abstract class BatchSpec extends FlatSpec with BeforeAndAfter {
  protected val master = "local[*]"
  protected val appName = "test"

  protected var spark: SparkSession = _
  protected var sc: SparkContext = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
      .set("spark.ui.enabled", "false")
      .set("spark.driver.host", "localhost")

    spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    sc = spark.sparkContext
  }

  after {
    if (sc != null) {
      sc.stop()
      sc = null
    }
  }
}
