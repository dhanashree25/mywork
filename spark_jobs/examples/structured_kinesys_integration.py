
from __future__ import print_function

import sys

from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

if __name__ == "__main__":
    awsAccessKey = "AKIAJVXOJOB7BQZ6GJGA"
    awsSecretKey = "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"

    spark = SparkSession.builder.appName(
        "StructuredFileWordCount").getOrCreate()

    kinesisDF = spark.readStream.format("kinesis").option("streamName", "qa-nlapi-kinesis-test").option("initialPosition", "earliest") \
        .option("region", "us-west-2").option("awsAccessKey", awsAccessKey).option("awsSecretKey", awsSecretKey).load()

    word = kinesisDF.select(explode(split(lines.value, "&")).alias("word"))

    # Generate running word count
    wordCounts = word.groupBy("word").count()

    # Start running the query that prints the running counts to the console
    query = wordCounts.writeStream.outputMode(
        "complete").format("console").start()

    query.awaitTermination()
