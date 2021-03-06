package com.diceplatform.brain

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.sql.{RuntimeConfig, _}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object implicits {
  private def dropSubColumn(col: Column, colType: DataType, fullColName: String, dropColName: String): Option[Column] = {
    // Drop column
    if (fullColName.equals(dropColName)) {
      return None
    }

    if (!dropColName.startsWith(s"$fullColName.")) {
      return Some(col)
    }

    // Recursion step
    colType match {
      // Structs
      case colType: StructType => Some(
        struct(
          colType.fields.flatMap(f => dropSubColumn(col.getField(f.name), f.dataType, s"$fullColName.${f.name}", dropColName) match {
            case Some(x) => Some(x.alias(f.name))
            case None    => None
          }) : _*
        )
      )

      // Array
      case colType: ArrayType =>
        colType.elementType match {
          case innerType: StructType =>
            Some(
              struct(
                innerType.fields.flatMap(f => dropSubColumn(col.getField(f.name), f.dataType, s"$fullColName.${f.name}", dropColName) match {
                  case Some(x) => Some(x.alias(f.name))
                  case None    => None
                }): _*
              )
            )
        }
    }
  }

  protected def dropNested(df: DataFrame, colName: String): DataFrame = {
    df.schema.fields.flatMap(f => {
        if (colName.startsWith(s"${f.name}.")) {
          dropSubColumn(col(f.name), f.dataType, f.name, colName) match {
            case Some(x) => Some((f.name, x))
            case None    => None
          }
        } else {
          None
        }
      })
      // Add columns back
      .foldLeft(df.drop(colName)) {
        case (df, (colName, column)) => df.withColumn(colName, column)
      }
  }


  implicit class ExtendedDataFrame(df: DataFrame) extends Serializable {
    /**
      * Drops nested field from DataFrame
      *
      * @param colName Dot-separated nested field name
      */
    def dropNested(colName: String): DataFrame = {
      implicits.dropNested(df, colName)
    }
  }

  private val JDBC_CONF_DRIVER_KEY = "spark.jdbc.driver"
  private val JDBC_CONF_DRIVER_DEFAULT = "com.amazon.redshift.jdbc.Driver"
  private val JDBC_CONF_URL_KEY = "spark.jdbc.url"
  private val JDBC_CONF_USER_KEY = "spark.jdbc.username"
  private val JDBC_CONF_PASSWORD_KEY = "spark.jdbc.password"
  private val REDSHIFT_TEMP = "spark.redshift.temp"

  implicit class ExtendedDataFrameWriter(dfw: DataFrameWriter[Row]) extends Serializable {
    /**
      * Get the redshift database connection for writing
      */
    def redshift(spark: SparkSession): DataFrameWriter[Row] = {
      dfw
        .format("com.databricks.spark.redshift")
        .option("forward_spark_s3_credentials", "true")
        .option("tempformat", "CSV")
        .option("tempdir", "s3n://" + spark.conf.get(REDSHIFT_TEMP, "qa-dicebrain-dce-test-temp-redshift"))
        .option("url", spark.conf.get(JDBC_CONF_URL_KEY))
        .option("user", spark.conf.get(JDBC_CONF_USER_KEY))
        .option("password", spark.conf.get(JDBC_CONF_PASSWORD_KEY))
    }
  }

  implicit class ExtendedDataFrameReader(dfr: DataFrameReader) extends Serializable {
    /**
      * Get the redshift database connection for reader
      */
    def redshift(spark: SparkSession): DataFrameReader = {
      dfr
        .format("com.databricks.spark.redshift")
        .option("forward_spark_s3_credentials", "true")
        .option("tempformat", "CSV")
        .option("tempdir", "s3n://" + spark.conf.get(REDSHIFT_TEMP, "qa-dicebrain-dce-test-temp-redshift"))
        .option("url", spark.conf.get(JDBC_CONF_URL_KEY))
        .option("user", spark.conf.get(JDBC_CONF_USER_KEY))
        .option("password", spark.conf.get(JDBC_CONF_PASSWORD_KEY))
    }

    /**
      * Read a file encoded with JSON objects on a single line
      */
    def jsonSingleLine(spark: SparkSession, path: String, schema: StructType): DataFrame = {
      val rdd = spark.sparkContext
        .hadoopFile(path, classOf[SingleJSONLineInputFormat], classOf[LongWritable], classOf[Text])
        .map(pair => pair._2.toString)
        .setName(path)

      spark.read
        .schema(schema)
        .json(spark.createDataset(rdd)(Encoders.STRING))
    }
  }
}