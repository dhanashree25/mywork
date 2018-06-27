package com.diceplatform.brain

import org.apache.spark.sql.types._

object Schema {
  val video = StructType(List(
    StructField("title", StringType, nullable=true),
    StructField("description", StringType, nullable=true),
    StructField("eventId", LongType, nullable=true),
    StructField("tournamentId", LongType, nullable=true),
    StructField("propertyId", LongType, nullable=true),
    StructField("sportId", LongType, nullable=true),
    StructField("deleted", BooleanType, nullable=true),
    StructField("geoRestriction", StructType(List(
      StructField("geoRestrictionType", StringType, nullable=true),
      StructField("countries", ArrayType(StringType, containsNull=true), nullable=true)
    )), nullable=true),
    StructField("updatedAt", TimestampType, nullable=true),
    StructField("startDate", TimestampType, nullable=true),
    StructField("endDate", TimestampType, nullable=true),
    StructField("thumbnailUrl", StringType, nullable=true),
    StructField("thumbnail", StringType, nullable=true),
    StructField("vodDveId", IntegerType, nullable=true),
    StructField("live", BooleanType, nullable=true),
    StructField("draft", BooleanType, nullable=true),
    StructField("duration", LongType, nullable=true),
    StructField("tags", ArrayType(StringType, containsNull=false), nullable=true)
  ))

  val data = StructType(List(
    StructField("ta", StringType, nullable=true),
    StructField("TA", StringType, nullable=true),
    StructField("cid", StringType, nullable=true),
    StructField("device", StringType, nullable=true),
    StructField("startedAt", LongType, nullable=true),
    StructField("v", video, nullable=true),
    StructField("vid", IntegerType, nullable=true)
  ))

  val payload = StructType(List(
    StructField("action", ByteType, nullable=true),
    StructField("cid", StringType, nullable=true),
    StructField("data", data, nullable=true),
    StructField("progress", LongType, nullable=true),
    StructField("video", LongType, nullable=true)
  ))

  val root = StructType(List(
    StructField("clientIp", StringType, nullable=true),
    StructField("country", StringType, nullable=true),
    StructField("customerId", StringType, nullable=true),
    StructField("payload", payload, nullable=true),
    StructField("realm", StringType, nullable=true),
    StructField("town", StringType, nullable=true),
    StructField("ts", TimestampType, nullable=true)
  ))
}
