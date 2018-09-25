# You can run this in pyspark with
# PYSPARK_DRIVER_PYTHON=ipython
# ~/bin/spark-2.2.0-bin-hadoop2.7/bin/pyspark --master local[4] --jars
# ~/bin/spark-2.2.0-bin-hadoop2.7/jars/spark-streaming-kinesis-asl_2.11-2.2.0.jar

from __future__ import print_function

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream


#sc = SparkContext(appName="NewApp")
ssc = StreamingContext(sc, 1)
lines = KinesisUtils.createStream(
    ssc, "testapp", "qa-nlapi-kinesis-test", "https://kinesis.eu-west-1.amazonaws.com",
    "eu-west-1", InitialPositionInStream.LATEST, 2)


def accept_message(message):
    """ Accept a system message

    Accept an AVRO JSON style message (textual format) and process it.

    Args:
        message: AVRO JSON format

    Returns:
        dict: JSON response

    """
    def decode(message):
        io_bytes = io.BytesIO(message)
        reader = DataFileReader(io_bytes, DatumReader())
        return [m for m in reader]
    if message is None:
        raise exception.InvalidMessageError("No message in request")

    try:
        decoded = decode(message)
    except AvroTypeException as exc:
        raise exception.InvalidMessageError(
            "Cannod decode message: {}".format(exc.message))
    self._log.debug("Received messages: {}".format(decoded))

    responses = []

    for m in decoded:
        r = Matcher.match(m)

        responses.append(r)
        json_responses.append(json.dumps(r))

    return responses

decoded_data = lines.map(lambda d: accept_message(d))


counts = lines.flatMap(
    lambda line: line.split(" ")).map(
        lambda word: (
            word,
            1)).reduceByKey(
                lambda a,
    b: a + b)
counts.pprint()


ssc.start()
ssc.awaitTermination()
