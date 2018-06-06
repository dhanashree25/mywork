# =====================================================================================================================
# Spark Job to read event messages from dice bucket, filter signins and 
# insert them into redshift user_logins table.
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
            if out["payload"]["action"]!=6: # 6 for Logins
                continue
            yield {
                "ta": out["payload"].get("data", {}).get("TA"),
                "realm": out["realm"],
                "customer_id": out.get("customerId"),
                "country": out.get("country"),
                "town": out.get("town"),
                "client_ip": out.get("clientIp"),
                "ts": datetime.datetime.strptime(out["ts"], "%Y-%m-%d %H:%M:%S")
                }


if __name__ == "__main__":
    import time
    start_time = time.time()
    # Initialize a SparkContext with a name
    sc = SparkSession.builder.appName("ReadLogins").getOrCreate()
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
    sc._jsc.hadoopConfiguration().set(
        "fs.s3n.awsAccessKeyId",
        config["access_key_id"])
    sc._jsc.hadoopConfiguration().set(
        "fs.s3n.awsSecretAccessKey",
        config["access_secret_key"])

    # Read S3 bucket files
    #readRdd = sc.sparkContext.textFile("testdata/test.json")
    readRdd = sc.sparkContext.textFile("s3n://dce-tracking/prod/2018/04/08/00/*")

    # Decode Json files
    jsonRdd = readRdd.flatMap(json_decoder_generator)  # repartition(100)
    # Specify the dataframe schema.. can be added to config
    schema = StructType([StructField("customer_id", StringType()),
                         StructField("realm", StringType()),
                         StructField("town", StringType()),
                         StructField("country", StringType()),
                         StructField("ts", TimestampType()),
                         StructField("client_ip", StringType()),
                         StructField("ta", StringType()),
                         ])
    signupDf = sqlc.createDataFrame(data=jsonRdd, schema=schema)

    # Write data to Redshift
    postgres_properties = {
        "user": "saffron",
        "password": "1Nn0v8t3",
        "driver": "com.amazon.redshift.jdbc.Driver"
    }
    try:
        signupDf.write.jdbc(
                 url="jdbc:postgresql://172.21.105.71:5439/redshift",
                 table="user_logins",
                 mode="append",
                 properties=postgres_properties)
    except Exception, e:
        print e
    print ("time- %s " % (time.time() - start_time))
