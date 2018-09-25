package com.diceplatform.brain

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec}

abstract class BatchSpec extends FlatSpec with BeforeAndAfter {
  protected val master = "local[*]"
  protected val appName = "test"

  protected var spark: SparkSession = _
  protected var sc: SparkContext = _

  val conf: SparkConf = new SparkConf()
    .setMaster(master)
    .setAppName(appName)
    .set("spark.ui.enabled", "false")
    .set("spark.driver.host", "localhost")

  before {
    spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    sc = spark.sparkContext
  }

  after {
    if (spark != null) {
      spark.stop()
      spark = null
    }
  }
}
