#=========================================================================
# Spark Job to Aggregate and insert Video play
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
    sc = SparkContext(appName="ReadVideoMessageStream")

    # Create a StreamingContext with a batch interval of 2 seconds
    batch_interval = 4

    # Create Streaming context
    ssc = StreamingContext(sc, batch_interval)

    # Checkpointing feature
    ssc.checkpoint("checkpoint")

    sqlContext = SQLContext(sc)

    # Source the data
#     data = ssc.textFileStream("file:////home/vagrant/testdata/")
    data = KinesisUtils.createStream(
        ssc, "kinesys-test17", "qa-nlapi-kinesis-test", "https://kinesis.eu-west-1.amazonaws.com",
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

#     filtered_stream = dataStream.filter(
#         lambda x: x['message_type'] == 'VIDEO_HB')
#     print filtered_stream.pprint()

    # Operations
    def video_heartbeat(x):
        return Row(user_id=x['user_id'],
                   content_id=x['product_id'],
                   duration=int(x['update_interval']),
                   experiment_id=x['experiments'][0].get("experiment_id"),
                   variant_id=x['experiments'][0].get("variant_id"),
                   # video_length=0,
                   event_datetime=datetime.datetime.now()
                   )
    
    def purchase_confirmation(w):
        return Row(price=int(
                    w['event_value']),
                    user_id=w['user_id'],
                    package_id=w['event_cat_sku'],
                    eventtime=datetime.datetime.now())
    
    def register_user(x):
        return Row( user_id=w['user_id'],
                    eventtime=w['event_time']
                    #datetime.datetime.now()
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
            map_selection = {'REGISTER_USER': register_user,
                             'VIDEO_HB': video_heartbeat,
                             'VIDEO_STOP': video_heartbeat,
                             'PURCHASE_CONFIRMATION':purchase_confirmation}
            #cut down rdd into 3 different rdds
            def video(x): return  x['message_type'] == 'VIDEO_HB'
            def app(x): return x['message_type'] == 'APP_EVENT'
            def purchase(x): return x.get('event_name') == 'PURCHASE_CONFIRMATION'
            def user(x): return x.get('event_name') == 'REGISTER_USER'
            
            #split and process
#             rdd_video, rdd_purchase = (rdd.filter(f).cache() for f in (video, purchase))
#             print rdd_video,rdd_purchase
            #rdd_purchase, rdd_user = (rdd_app.filter(f) for f in (purchase, user))

            rdd.cache()
            #write purcahse data
            rdd_video = rdd.filter(lambda x: x.get('message_type') == 'VIDEO_HB')
            rowVideo = rdd_video.map(lambda x: map_selection[x['message_type']](x))
            rowDataFrame = spark.createDataFrame(rowVideo)
            rowDataFrame.show()
#             rowDataFrame.write.jdbc(
#                 url=postgres_url,
#                 table='purchases',
#                 mode="append",
#                 properties=postgres_properties)
#             
            #write video data
#             print rdd_video.collect()
#             rdd_video.cache()
            #rowVideo = rdd_video.map(lambda x: map_selection[x['message_type']](x))
            videoDataFrame = spark.createDataFrame(rowVideo)
            videoDataFrame.show()
            stream_data = videoDataFrame.select(
                'user_id', 'content_id', 'experiment_id', 'variant_id', 'duration','event_datetime')

            table_data = spark.read.jdbc(
                url=postgres_url,
                table='video_play',
                properties=postgres_properties)
            print table_data
            joined_data = stream_data.union(table_data)
            final_dataframe = joined_data.groupBy(
                "user_id", "content_id", "experiment_id", "variant_id").agg(
                func.sum('duration').alias('duration'))

            final_dataframe.persist()
            final_dataframe.show()
            # Write to database
#             final_dataframe.write.jdbc(
#                 url=postgres_url,
#                 table='video_play',
#                 mode=mode,
#                 properties=postgres_properties)

            final_dataframe.unpersist()
        except Exception as e:
            print e

# Stream Processing
    def process_next(time, rdd):
        postgres_url = "jdbc:postgresql://quicksight.ctimznjpq2f0.eu-west-1.rds.amazonaws.com:5432/analytics"
        postgres_properties = {
            "user": "saffron",
            "password": "1nn0v8t3",
            "driver": "org.postgresql.Driver"
        }

        mode = 'overwrite'
        print("========= %s =========" % str(time))
        map_selection = {'REGISTER_USER': register_user,
                             'VIDEO_HB': video_heartbeat,
                             'VIDEO_STOP': video_heartbeat,
                             'PURCHASE_CONFIRMATION':purchase_confirmation}
        try:
            # Get the singleton instance of SparkSession
            spark = getSparkSessionInstance(rdd.context.getConf())
            print rdd.collect()
            rdd_purchase = rdd.filter(lambda x: x.get('event_name') == 'PURCHASE_CONFIRMATION').cache()
            print rdd_purchase.collect()
            rowPurchase = rdd_purchase.map(lambda w: Row(price=int(
                    w['event_value']),
                    user_id=w['user_id'],
                    package_id=w['event_cat_sku'],
                    eventtime=datetime.datetime.now())).cache()
            #rowPurchase = rdd_purchase.map(lambda x: map_selection[x['event_name']](x)).cache()
            rowDataFrame = spark.createDataFrame(rowPurchase)
            rowDataFrame.show()
#             rowDataFrame.write.jdbc(
#                 url=postgres_url,
#                 table='purchases',
#                 mode="append",
#                 properties=postgres_properties)
        except Exception as e:
            print e

    # Convert stream into RDD and insert into db
    dataStream.foreachRDD(process)
    dataStream.foreachRDD(process_next)
    # Start the computation
    ssc.start()

    # Wait for the computation to terminate
    ssc.awaitTermination()
