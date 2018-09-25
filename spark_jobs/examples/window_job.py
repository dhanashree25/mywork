# Parse the line from the socket, and save the remainder of the line to a table
# the first word must be either ONE, TWO, or something else
# depending on the first word, we save to tables one, two, or other respectively

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)

lines_dstream = ssc.socketTextStream("127.0.0.1", 8001)


def save_line(partition):
    for line_pair in partition:
        split_pair = line_pair.split(" ", 1)
        default = "table_other"
        tables = {
            "ONE": "table_one",
            "TWO": "table_two",
        }
        print("Saving pair {}, to table {}".format(split_pair, tables.get(split_pair[0], default)))
    return ""

ret = lines_dstream.foreachRDD(lambda rdd: rdd.foreachPartition(save_line))

ssc.start()
ssc.awaitTermination()
