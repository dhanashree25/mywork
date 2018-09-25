from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

if __name__ == "__main__":
    spark = SparkSession.builder.appName("StructuredFileWordCount").getOrCreate()

    lines = spark.readStream.text("s3a://qa-nlapi-spark/testdata/")

    # Split the lines into words
    words = lines.select(explode(split(lines.value, "&")).alias("word"))

    # Generate running word count
    wordCounts = words.groupBy("word").count()

    # Start running the query that prints the running counts to the console
    query = wordCounts.writeStream.outputMode("complete").format("console").start()

    query.awaitTermination()
