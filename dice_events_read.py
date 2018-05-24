# ============= =======================================================================================================
# Spark Job to read event messages from dice bucket and insert into database
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
            yield {
                "action": out.get("action"),
                "client_ip": out.get("clientIp"),
                "action": float(out["payload"]["action"]),
                "session_id": out["payload"].get("data", {}).get("cid"),
                "device": out["payload"].get("data", {}).get("device"),
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
    sc = SparkSession.builder.appName("ReadDiceData").getOrCreate()
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
    mount_name = 'dce-tracking'
    sc._jsc.hadoopConfiguration().set(
        "fs.s3n.awsAccessKeyId",
        config["access_key_id"])
    sc._jsc.hadoopConfiguration().set(
        "fs.s3n.awsSecretAccessKey",
        config["access_secret_key"])

    # Read S3 bucket files
    readRdd = sc.sparkContext.textFile("testdata/test.json")
    #readRdd = sc.sparkContext.textFile("s3n://dce-tracking/prod/2018/05/01/*")

    # Decode Json files
    jsonRdd = readRdd.flatMap(json_decoder_generator)  # repartition(100)
    # Specify the dataframe schema.. can be added to config
    schema = StructType([StructField("customer_id", StringType()),
                         StructField("video_id", IntegerType()),
                         StructField("started_at", TimestampType()),
                         StructField("realm", StringType(), True),
                         StructField("town", StringType()),
                         StructField("country", StringType()),
                         StructField("session_id", StringType(), True),
                         StructField("client_ip", StringType()),
                         StructField("action", DoubleType(), True),
                         StructField("device", StringType()),
                         StructField("progress", IntegerType(), True),
                         StructField("ts", TimestampType(), True),
                         StructField("ta", StringType()),
                         ])
    rowDataFrame = sqlc.createDataFrame(data=jsonRdd, schema=schema)
    # Write to database
    rowDataFrame.write.jdbc(
        url=postgres_url,
        table=config["tablename_events"],
        mode=mode,
        properties=postgres_properties)
    print ("time- %s " % (time.time() - start_time))
