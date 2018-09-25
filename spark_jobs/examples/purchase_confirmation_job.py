#=========================================================================
# Spark Job to insert purchase data
# Version- 1.0
#=========================================================================
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
    sc = SparkContext(appName="ReadAppMessageStream")

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
        ssc, "kinesys_app_event", "qa-nlapi-kinesis-test", "https://kinesis.eu-west-1.amazonaws.com",
        "eu-west-1", InitialPositionInStream.LATEST, batch_interval, decoder=avro_decoder)

    # Avro df usage
    # - msgs - collection of individual records
    # - msg - one record places on the stack via putrecord
    # - list(msgs) - creates a dict accessible via msg['data']['messages'] etc.
    # - ^ this is because DataFileReader returns an iterator of dicts

    dataStream = data.flatMap(lambda msgs: list(msgs)).map(lambda msg: msg)

    filtered_stream = dataStream.filter(
        lambda x: x['message_type'] == 'APP_EVENT')
    print filtered_stream.pprint()

    # Operations
    def purchase_confirmation(w):
        return Row(price=int(
            w['event_value']),
            user_id=w['user_id'],
            package_id=w['event_cat_sku'],
            eventtime=datetime.datetime.now())

    # Stream Processing
    def process(time, rdd):
        postgres_url = "jdbc:postgresql://quicksight.ctimznjpq2f0.eu-west-1.rds.amazonaws.com:5432/analytics"
        postgres_properties = {
            "user": "saffron",
            "password": "1nn0v8t3",
            "driver": "org.postgresql.Driver"
        }
        table_name = 'purchases'
        mode = 'append'
        print("========= %s =========" % str(time))
        try:
            # Get the singleton instance of SparkSession
            spark = getSparkSessionInstance(rdd.context.getConf())

            # Convert RDD[String] to RDD[Row] to DataFrame
            rowRdd = rdd.map(lambda x: Row(price=int(x['event_value']),
                                           user_id=x['user_id'],
                                           package_id=x['event_cat_sku'],
                                           eventtime=datetime.datetime.now()))

            rowDataFrame = spark.createDataFrame(rowRdd)

            rowDataFrame.persist()
            rowDataFrame.show()
            # Write to database
            rowDataFrame.write.jdbc(
                url=postgres_url,
                table=table_name,
                mode=mode,
                properties=postgres_properties)

            rowDataFrame.unpersist()
        except Exception as e:
            print e

    # Convert stream into RDD and insert into db
    filtered_stream.foreachRDD(process)

    # Start the computation
    ssc.start()

    # Wait for the computation to terminate
    ssc.awaitTermination()
