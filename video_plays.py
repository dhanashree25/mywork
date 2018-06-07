# =====================================================================================================================
# Spark Job to read event messages from dice bucket, calculate video play durations and 
# insert them into redshift signups table.
# Version-1.0
# =====================================================================================================================
import datetime
import json
from io import BytesIO

from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
import pyspark.sql.functions as func
from pyspark.sql import SQLContext
from pyspark.sql.types import *


def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']


def json_decoder_generator(contents):
    pos = 0
    while True:
        trial = contents.find('}', pos)
        if trial == -1:
            return
        trial_json = contents[: trial + 1]
        try:
            out = json.loads(trial_json)
        except ValueError:
            pos = trial + 1
        else:
            contents = contents[trial + 1:]
            pos = 0
            if out["payload"]["action"]!= 2:
                continue
            yield {
                "session_id": out["payload"].get("data", {}).get("cid"),
                "video_id": out["payload"].get("video"),
                "realm": out["realm"],
                "customer_id": out.get("customerId"),
                "country": out.get("country"),
                "town": out.get("town"),
                "progress": out["payload"].get("progress"),
                "ts": datetime.datetime.strptime(out["ts"], "%Y-%m-%d %H:%M:%S"),
                "started_at": datetime.datetime.utcfromtimestamp(out["payload"].get("data", {}).get("startedAt", 0) / 1000) if isinstance(out["payload"].get("data", {}).get("startedAt"), int) else None,
                }


if __name__ == "__main__":
    import time
    start_time = time.time()
    # Initialize a SparkContext with a name
    sc = SparkSession.builder.appName("VideoPlays").getOrCreate()
    # Initialize SQL context
    sqlc = SQLContext(sc)
    # get_config
    config_file = sc.sparkContext.textFile('s3n://test-dce-spark/config.txt')
    config = config_file.map(lambda x: (
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

    # bucket details
    bucket_name = 's3n://temp-redshift-events'
    sc._jsc.hadoopConfiguration().set(
        "fs.s3n.awsAccessKeyId",
        config["access_key_id"])
    sc._jsc.hadoopConfiguration().set(
        "fs.s3n.awsSecretAccessKey",
        config["access_secret_key"])

    # Read S3 bucket files
    #readRdd = sc.sparkContext.textFile("testdata/test.json")
    readRdd = sc.sparkContext.textFile("s3n://dce-tracking/prod/2018/04/12/07/*")

    # Decode Json files
    jsonRdd = readRdd.flatMap(json_decoder_generator)  # repartition(100)
    # Specify the dataframe schema.. can be added to config
    schema = StructType([StructField("customer_id", StringType()),
                         StructField("session_id", StringType()),
                         StructField("realm", StringType()),
                         StructField("town", StringType()),
                         StructField("country", StringType()),
                         StructField("started_at", TimestampType()),
                         StructField("ts", TimestampType()),
                         StructField("video_id", StringType()),
                         StructField("progress", IntegerType()),
                         ])
    eventsDf = sqlc.createDataFrame(data=jsonRdd, schema=schema)
    list_of_session_ids= tuple(set(jsonRdd.map(lambda i: str(i["session_id"])).collect()))
   # print list_of_session_ids
    query = """select * from video_plays where session_id in {}""".format(list_of_session_ids)

    historydf = sqlc.read \
        .format("com.databricks.spark.redshift") \
        .option("url", "jdbc:redshift://172.21.105.71:5439/redshift?user=saffron&password=1Nn0v8t3") \
        .option("query", query) \
        .option("tempdir", bucket_name) \
        .option('forward_spark_s3_credentials', True) \
        .load()
    
    videodf = eventsDf.groupBy(
        "session_id", "customer_id", "realm", "video_id", "started_at", "town", "country").agg(func.max('progress').alias('duration'), func.max('ts').alias('end_time'), func.min('ts').alias('start_time'))
    
    unionDf = videodf.union(historydf)
    
#     unionDf.show()
    
    finalDf = unionDf.groupBy(
        "session_id", "customer_id", "realm", "video_id", "started_at", "town", "country").agg(func.max('duration').alias('duration'), func.max('end_time').alias('end_time'), func.min('start_time').alias('start_time'))
    
#     finalDf.show()
    
    # Write data to Redshift
    postgres_properties = {
        "user": "saffron",
        "password": "1Nn0v8t3",
        "driver": "com.amazon.redshift.jdbc.Driver"
    }
    try:
        videodf.write.jdbc(
                 url="jdbc:postgresql://172.21.105.71:5439/redshift",
                 table="video_plays",
                 mode="append",
                 properties=postgres_properties)
    except Exception, e:
        print e
    print ("time- %s " % (time.time() - start_time))
