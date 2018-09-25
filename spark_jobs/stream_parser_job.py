# =========================================================================
# Spark Job to parse messages from stream, process and insert into
# datastructure
# Version- 2.0
# =========================================================================
from  dateutil import parser
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
from pyspark.sql.types import *


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

    # get_config
    config_file = sc.textFile('s3n://qa-nlapi-spark-emr/config.txt')
    config = config_file.map(
        lambda x: (
            x.split(",")[0],
            x.split(",")[1])).collectAsMap()

    # database conn details
    postgres_url = config["postgres_url"]
    postgres_properties = {
        "user": config["postgres_user"],
        "password": config["postgres_password"],
        "driver": config["postgres_driver"]
    }
    mode = 'append'

    # get experiment and variant ids
    exp_data = sc.textFile('s3n://qa-nlapi-spark-emr/experiments.txt')
    experiment_variants = exp_data.map(
        lambda x: (x.split(",")[0],
                   x.split(",")[1])).collectAsMap()

    # Source the data
    data = KinesisUtils.createStream(ssc,
                                     config["kinesis_app_name"],
                                     config["kinesis_stream_name"],
                                     config["kinesis_endpoint_url"],
                                     config["kinesis_region_name"],
                                     InitialPositionInStream.LATEST,
                                     batch_interval,
                                     decoder=avro_decoder)

    # Avro df usage
    # - msgs - collection of individual records
    # - msg - one record places on the stack via putrecord
    # - list(msgs) - creates a dict accessible via msg['data']['messages'] etc.
    # - ^ this is because DataFileReader returns an iterator of dicts

    dataStream = data.flatMap(lambda msgs: list(msgs)).map(lambda msg: msg)

    # Stream Processing
    def process_register(time, rdd):
        print("========= register %s =========" % str(time))
        try:
            # Get the singleton instance of SparkSession
            spark = getSparkSessionInstance(rdd.context.getConf())

            # Convert RDD[String] to RDD[Row] to DataFrame
            rdd_register = rdd.filter(
                lambda x: x.get('event_name') == 'REGISTER_SUCCESS')
            print rdd_register.collect()
            rowRegister = rdd_register.map(lambda x: Row(
                user_id=x['user_id'],
                event_datetime=datetime.datetime.now(),
                geo_latitude=float(
                    181 if x.get('latitude') is None else x.get('latitude')),
                geo_longitude=float(
                    361 if x.get('longitude') is None else x.get('longitude')),
                experiment_id=experiment_variants.get(x['experiments'][0].get("experiment_id"), "") if
                len(x['experiments']) > 0 else "",
                variant_id=experiment_variants.get(x['experiments'][0].get("variant_id"), "")if
                len(x['experiments']) > 0 else "",
            )).cache()

            rowDataFrame = spark.createDataFrame(rowRegister)
            rowDataFrame.show()
            rowDataFrame.write.jdbc(
                url=postgres_url,
                table='register_user',
                mode=mode,
                properties=postgres_properties)
        except Exception as e:
            print e

    def process_video(time, rdd):
        print("========= video - %s =========" % str(time))
        try:
            # Get the singleton instance of SparkSession
            spark = getSparkSessionInstance(rdd.context.getConf())

            # Convert RDD[String] to RDD[Row] to DataFrame
            rdd_video = rdd.filter(
                lambda x: x.get('message_type') == 'VIDEO_HB')

            rowRdd = rdd_video.map(lambda x: Row(user_id=x['user_id'],
                                                 content_id=x.get(
                                                     'product_id'),
                                                 duration=int(
                                                     x.get('update_interval', 0)),
                                                 experiment_id=experiment_variants.get(x['experiments'][0].get("experiment_id"), "") if
                                                 len(x['experiments']) > 0 else "",
                                                 variant_id=experiment_variants.get(x['experiments'][0].get("variant_id"), "")if
                                                 len(x['experiments']) > 0 else "",
                                                 event_datetime=datetime.datetime.now(),
                                                 geo_latitude=float(
                181 if x.get('latitude') is None else x.get('latitude')),
                geo_longitude=float(
                361 if x.get('longitude') is None else x.get('longitude')),
            ))

            rowDataFrame = spark.createDataFrame(rowRdd)
            stream_data = rowDataFrame.select(
                'user_id', 'content_id', 'experiment_id', 'variant_id', 'geo_latitude', 'geo_longitude', 'duration', 'event_datetime')

            table_data = spark.read.jdbc(
                url=postgres_url,
                table='video_play',
                properties=postgres_properties)
            max_table_data = table_data.groupBy(
                "user_id", "content_id", "experiment_id", "variant_id", "geo_latitude", "geo_longitude").agg(func.max('duration').alias('duration'), func.max('event_datetime').alias('event_datetime'))
            # max_table_data.show()
            joined_data = stream_data.union(max_table_data)

            final_dataframe = joined_data.groupBy(
                "user_id", "content_id", "experiment_id", "variant_id", "geo_latitude", "geo_longitude").agg(
                func.sum('duration').alias('duration'), func.max('event_datetime').alias('event_datetime'))

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

    def process_purchase(time, rdd):
        print("========= purchase %s =========" % str(time))
        try:
            # Get the singleton instance of SparkSession
            spark = getSparkSessionInstance(rdd.context.getConf())

            # Convert RDD[String] to RDD[Row] to DataFrame
            rdd_purchase = rdd.filter(
                lambda x: x.get('event_name') == 'PURCHASE_CONFIRMATION')

            rowPurchase = rdd_purchase.map(lambda x: Row(price=int(
                x['event_value']),
                user_id=x['user_id'],
                package_id=x['event_cat_sku'],
                eventtime=datetime.datetime.now(),
                geo_latitude=float(
                181 if x.get('latitude') is None else x.get('latitude')),
                geo_longitude=float(
                361 if x.get('longitude') is None else x.get('longitude')),
                experiment_id=experiment_variants.get(x['experiments'][0].get("experiment_id"), "") if
                len(x['experiments']) > 0 else "",
                variant_id=experiment_variants.get(x['experiments'][0].get("variant_id"), "")if
                len(x['experiments']) > 0 else "",
            )).cache()

            rowDataFrame = spark.createDataFrame(rowPurchase)
            rowDataFrame.show()
            rowDataFrame.write.jdbc(
                url=postgres_url,
                table='purchases',
                mode=mode,
                properties=postgres_properties)
        except Exception as e:
            print e

    def process_rating(time, rdd):
        print("========= ratings %s =========" % str(time))
        try:
            # Get the singleton instance of SparkSession
            spark = getSparkSessionInstance(rdd.context.getConf())

            # Convert RDD[String] to RDD[Row] to DataFrame
            rdd_rating = rdd.filter(
                lambda x: x.get('event_name') == 'STREAM_RATING')
            print rdd_rating.collect()
            rowRating = rdd_rating.map(lambda x: Row(rating=int(
                x['event_value']),
                user_id=x['user_id'],
                content_id=x['product_id'],
                event_datetime=datetime.datetime.now(),
                geo_latitude=float(
                181 if x.get('latitude') is None else x.get('latitude')),
                geo_longitude=float(
                361 if x.get('longitude') is None else x.get('longitude')),
                experiment_id=experiment_variants.get(x['experiments'][0].get("experiment_id"), "") if
                len(x['experiments']) > 0 else "",
                variant_id=experiment_variants.get(x['experiments'][0].get("variant_id"), "")if
                len(x['experiments']) > 0 else "",
            )).cache()

            rowDataFrame = spark.createDataFrame(rowRating)
            rowDataFrame.show()
            rowDataFrame.write.jdbc(
                url=postgres_url,
                table='ratings',
                mode=mode,
                properties=postgres_properties)
        except Exception as e:
            print e

    def process_email(time, rdd):
        print("========= emails %s =========" % str(time))
        try:
            # Get the singleton instance of SparkSession
            spark = getSparkSessionInstance(rdd.context.getConf())

            # Convert RDD[String] to RDD[Row] to DataFrame
            rdd_email = rdd.filter(
                lambda x: x.get('role') == 'email-delivery')
            print rdd_email.collect()
            rowRating = rdd_email.map(lambda x: Row(message_id=x.get('message_id') if x.get('message_id') is not None else "",
						    provider_message_id=x.get("provider_message_id") if x.get("provider_message_id") is not None else "",
                                                    recipient_id=x.get('recipient') if x.get('recipient') is not None else "",
                                                    email_status=x.get('cmd'),
                                                    status_code=x.get('status_code') if x.get('status_code') is not None else "",
                                                    status_message=x.get('data') if x.get('data') is not None else "",
                                                    event_timestamp=parser.parse([v for k,v in x.iteritems() if k.endswith('timestamp')][0])
)).cache()

            rowDataFrame = spark.createDataFrame(rowRating)
            rowDataFrame.show()
            rowDataFrame.write.jdbc(
                url=postgres_url,
                table='email_status',
                mode=mode,
                properties=postgres_properties)
        except Exception as e:
            print e

    # Convert stream into RDD and insert into db
    dataStream.foreachRDD(process_register)
    dataStream.foreachRDD(process_video)
    dataStream.foreachRDD(process_purchase)
    dataStream.foreachRDD(process_rating)
    dataStream.foreachRDD(process_email)
    # Start the computation
    ssc.start()

    # Wait for the computation to terminate
    ssc.awaitTermination()
