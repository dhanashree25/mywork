# Download postgres connetor from https://jdbc.postgresql.org/download.html
# bin/spark-submit --py-files ~/avro-1.8.2.zip  --jars
# jars/postgresql-42.1.4.jar,jars/spark-streaming-kinesis-asl-assembly_2.11-2.0.0-preview.jar
# ~/spark_jobs/test_stream_jdbc.py

import datetime

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

from io import BytesIO

from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
import pyspark.sql.functions as func
from pyspark.sql import SQLContext


def avro_decoder(s):
    if s is None:
        return None
    return DataFileReader(BytesIO(s), DatumReader())


def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']


if __name__ == "__main__":
    # Initialize a SparkContext with a name
    sc = SparkContext(appName="MessageStream")

    # Create a StreamingContext with a batch interval of 2 seconds
    batch_interval = 2

    # Create Streaming context
    ssc = StreamingContext(sc, batch_interval)

    # Checkpointing feature
    ssc.checkpoint("checkpoint")

    sqlContext = SQLContext(sc)

    # Source the data
#     data = ssc.textFileStream("file:////home/vagrant/testdata/")
    data = KinesisUtils.createStream(
        ssc, "test1", "test-stream", "https://kinesis.eu-west-1.amazonaws.com",
        "eu-west-1", InitialPositionInStream.LATEST, batch_interval)

    # Avro df usage
    # - msgs - collection of individual records
    # - msg - one record places on the stack via putrecord
    # - list(msgs) - creates a dict accessible via msg['data']['messages'] etc.
    # - ^ this is because DataFileReader returns an iterator of dicts
#     dataStream = data.flatMap(lambda msgs: list(msgs)).map(lambda msg: msg)
    print data
    dataStream = data.flatMap(lambda msgs: list(msgs)).map(lambda msg: msg)
    print "waiting for message "
    print dataStream
    counts = dataStream.count()
    counts.pprint()
    print " is there anything above this? "
    # Start the computation
    ssc.start()

    # Wait for the computation to terminate
    ssc.awaitTermination()
