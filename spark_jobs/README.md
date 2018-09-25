### Creating the avro zip file
1. Download avro release from http://www.apache.org/dyn/closer.cgi/avro/ for python
2. Re-zip so that contents of avro-x.y.z.zip are structured so that src/avro/* becomes avro/* in the included zip file - note it seems to have to be zip instead of tar

### Commandline pyspark run

Avro path may have to be absolute or run from inside this git repo
```
pyspark --packages org.apache.spark:spark-streaming-kinesis-asl_2.11:2.2.1 --py-files avro-1.8.2.zip
```
