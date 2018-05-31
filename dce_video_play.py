# =====================================================================================================================
# Spark Job to read event messages from redshift and insert process video views
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


if __name__ == "__main__":
    import time
    start_time = time.time()
    # Initialize a SparkContext with a name
    sc = SparkSession.builder.appName("ReadRedshiftData").getOrCreate()
    # Initialize SQL context
    sqlc = SQLContext(sc)
    # get_config
    config_file = sc.sparkContext.textFile('s3n://test-dce-spark/config.txt')
    config = config_file.map(lambda x: (
        x.split(",")[0],
        x.split(",")[1])).collectAsMap()

    # bucket details
    bucket_name = 's3n://temp-redshift-events'
    sc._jsc.hadoopConfiguration().set(
        "fs.s3n.awsAccessKeyId",
        config["neulion_access_key_id"])
    sc._jsc.hadoopConfiguration().set(
        "fs.s3n.awsSecretAccessKey",
        config["neulion_access_secret_key"])

    # Read data from Redshift table
    eventsdf = sqlc.read \
        .format("com.databricks.spark.redshift") \
        .option("url", "jdbc:redshift://qa-redshift-cluster.camxxuuvbchc.eu-west-1.redshift.amazonaws.com:5439/redshift?user=saffron&password=1Nn0v8t3") \
        .option("dbtable", "actions") \
        .option("tempdir", bucket_name) \
        .option('forward_spark_s3_credentials', True) \
        .load()

    eventsdf.show()

    print ("time- %s " % (time.time() - start_time))
