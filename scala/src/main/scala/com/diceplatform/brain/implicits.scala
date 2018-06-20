package com.diceplatform.brain

import org.apache.spark.sql._
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

    def normalize(): DataFrame = Normalize.transform(df)
  }
}