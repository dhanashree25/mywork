#=========================================================================
# Spark Job to Aggregate and insert Video play
# Version- 1.5
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
    # get experiment data
    def get_experiments_data():
        #         conn_string = "host='qa-nlapi-db-postgres.sd-ngp.net' dbname='experiment' user='saffron' password='1nn0v8t3'"
        #         conn = psycopg2.connect(conn_string)
        #         cursor = conn.cursor()
        #         cursor.execute("select uuid, name from experiment union select uuid, name from experiment_variant;")
        #         records = cursor.fetchall()
        #         print records
        #         return dict(records)
        experiments = {}
        with open('s3a://qa-nlapi-emr/experiments.txt') as f:  # 's3://qa-nlapi-emr/experiments.txt'
            for line in f:
                fields = line.split(',')
                experiments[fields[0]] = fields[1]
        return experiments

    # Initialize a SparkContext with a name
    sc = SparkContext(appName="ReadVideoMessageStream")

    # Create a StreamingContext with a batch interval of 2 seconds
    batch_interval = 2

    # Create Streaming context
    ssc = StreamingContext(sc, batch_interval)

    # Checkpointing feature
    ssc.checkpoint("checkpoint")

    sqlContext = SQLContext(sc)

    #experiment_variants = sc.broadcast(get_experiments_data())
    exp_data = sc.textFile('s3n://qa-nlapi-emr/experiments.txt')
    experiment_variants = exp_data.map(lambda x: (x.split(",")[0], x.split(",")[1])).collectAsMap()
    print experiment_variants
    # Source the data
    data = KinesisUtils.createStream(
        ssc, "kinesys-test_23", "qa-nlapi-kinesis-test", "https://kinesis.eu-west-1.amazonaws.com",
        "eu-west-1", InitialPositionInStream.LATEST, batch_interval, decoder=avro_decoder)

    # Avro df usage
    # - msgs - collection of individual records
    # - msg - one record places on the stack via putrecord
    # - list(msgs) - creates a dict accessible via msg['data']['messages'] etc.
    # - ^ this is because DataFileReader returns an iterator of dicts

    def open_list(s):
        dict = {}
        for i in s:
            x = i.split("=")
            dict.update({x[0]: x[1]})
        return dict
    dataStream = data.flatMap(lambda msgs: list(msgs)).map(lambda msg: msg)

    # Operations
    def video_heartbeat(x):
        return Row(user_id=x['user_id'],
                   content_id=x['product_id'],
                   duration=int(x['update_interval']),
                   experiment_id=experiment_variants[x['experiments'][0].get(
                       "experiment_id")] if len(x['experiments']) > 0 else None,
                   variant_id=experiment_variants[x['experiments'][0].get(
                       "variant_id")]if len(x['experiments']) > 0 else None,
                   )

    # Stream Processing
    def process(time, rdd):
        postgres_url = "jdbc:postgresql://quicksight.ctimznjpq2f0.eu-west-1.rds.amazonaws.com:5432/analytics"
        postgres_properties = {
            "user": "saffron",
            "password": "1nn0v8t3",
            "driver": "org.postgresql.Driver"
        }

        mode = 'append'
        print("========= %s =========" % str(time))
        try:
            # Get the singleton instance of SparkSession
            spark = getSparkSessionInstance(rdd.context.getConf())

            # Convert RDD[String] to RDD[Row] to DataFrame
            print rdd.collect()
            rowRdd = rdd.map(lambda x: Row(user_id=x['user_id'],
                                           content_id='register' if x.get(
                                               'event_name') == 'REGISTER_SUCCESS' else x.get('product_id'),
                                           duration=int(x.get('update_interval', 0)),
                                           experiment_id=experiment_variants.get(x['experiments'][0].get("experiment_id"), "") if
                                           len(x['experiments']) > 0 else "",
                                           variant_id=experiment_variants.get(x['experiments'][0].get("variant_id"), "")if
                                           len(x['experiments']) > 0 else "",
                                           event_datetime=datetime.datetime.now()
                                           ))

            rowDataFrame = spark.createDataFrame(rowRdd)
            rowDataFrame.show()
            stream_data = rowDataFrame.select(
                'user_id', 'content_id', 'experiment_id', 'variant_id', 'duration', 'event_datetime')

            table_data = spark.read.jdbc(
                url=postgres_url,
                table='video_play',
                properties=postgres_properties)
            max_table_data = table_data.groupBy(
                "user_id", "content_id", "experiment_id", "variant_id").agg(func.max('duration').alias('duration'), func.max('event_datetime').alias('event_datetime'))
            max_table_data.show()
            joined_data = stream_data.union(max_table_data)

            final_dataframe = joined_data.groupBy(
                "user_id", "content_id", "experiment_id", "variant_id").agg(
                func.sum('duration').alias('duration'), func.max('event_datetime').alias('event_datetime'))

#             # final_df group by user_id and change duration in user_id+ register-content_id
#             final_dataframe = pre_dataframe.groupBy("user_id").agg(func.datediff(func.max('event_datetime'),func.min('event_datetime')))#.withColumn("duration", \
#               #func.when(pre_dataframe["content_id"] == 'register', "duration"))
#

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
    dataStream.foreachRDD(process)

    # Start the computation
    ssc.start()

    # Wait for the computation to terminate
    ssc.awaitTermination()
