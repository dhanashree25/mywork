#=========================================================================
# Spark Job to Aggregate and insert Video play
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
    sc = SparkContext(appName="ReadVideoMessageStream")

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
        ssc, "kinesys-test", "qa-nlapi-kinesis-test", "https://kinesis.eu-west-1.amazonaws.com",
        "eu-west-1", InitialPositionInStream.LATEST, batch_interval, decoder=avro_decoder)

    # Avro df usage
    # - msgs - collection of individual records
    # - msg - one record places on the stack via putrecord
    # - list(msgs) - creates a dict accessible via msg['data']['messages'] etc.
    # - ^ this is because DataFileReader returns an iterator of dicts
#     dataStream = data.flatMap(lambda msgs: list(msgs)).map(lambda msg: msg)

    def open_list(s):
        dict = {}
        for i in s:
            x = i.split("=")
            dict.update({x[0]: x[1]})
        return dict
    dataStream = data.flatMap(lambda msgs: list(msgs)).map(lambda msg: msg)
#     dataStream1 = data.map(lambda x: x.split("&"))
#     dataStream = dataStream1.map(open_list)

    filtered_stream = dataStream.filter(
        lambda x: x['message_type'] == 'VIDEO_HB')
    print filtered_stream.pprint()

    # Operations
    def video_heartbeat(x):
        return Row(user_id=x['user_id'],
                   content_id=x['product_id'],
                   duration=int(x['update_interval']),
                   experiment_id=x['experiments'][0].get("experiment_id"),
                   variant_id=x['experiments'][0].get("variant_id"),
                   # video_length=0,
                   # eventtime=datetime.datetime.now()
                   )

    # Stream Processing
    def process(time, rdd):
        postgres_url = "jdbc:postgresql://quicksight.ctimznjpq2f0.eu-west-1.rds.amazonaws.com:5432/analytics"
        postgres_properties = {
            "user": "saffron",
            "password": "1nn0v8t3",
            "driver": "org.postgresql.Driver"
        }

        mode = 'overwrite'
        print("========= %s =========" % str(time))
        try:
            # Get the singleton instance of SparkSession
            spark = getSparkSessionInstance(rdd.context.getConf())

            # Convert RDD[String] to RDD[Row] to DataFrame
            map_selection = {'APP_EVENT': app_event,
                             'VIDEO_HB': video_heartbeat,
                             'VIDEO_STOP': video_heartbeat}

            rowRdd = rdd.map(lambda x: map_selection[x['message_type']](x))

            rowDataFrame = spark.createDataFrame(rowRdd)

            stream_data = rowDataFrame.select(
                'user_id', 'content_id', 'experiment_id', 'variant_id', 'duration')

            table_data = spark.read.jdbc(
                url=postgres_url,
                table='video_play',
                properties=postgres_properties)

            joined_data = stream_data.union(table_data)

            final_dataframe = joined_data.groupBy(
                "user_id", "content_id", "experiment_id", "variant_id").agg(
                func.sum('duration').alias('duration'))

            final_dataframe.persist()
            final_dataframe.show()
            # Write to database
            final_dataframe.write.jdbc(
                url=postgres_url,
                table='video_play',
                mode=mode,
                properties=postgres_properties)

            final_dataframe.unpersist()
        except Exception as e:
            print e

    # Convert stream into RDD and insert into db
    filtered_stream.foreachRDD(process)

    # Start the computation
    ssc.start()

    # Wait for the computation to terminate
    ssc.awaitTermination()
