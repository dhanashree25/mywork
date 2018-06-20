# Read me

## Dependencies

- Scala 1.11
- SBT 1.1.
- Java 8
- Spark 2.3


## Packaging JAR file

    sbt package


## Stand Alone


    export SPARK_VERSION="2.3.1"
    export SPARK_MASTER_HOST="127.0.0.1"

    brew install spark

    mkdir "/usr/local/Cellar/apache-spark/${SPARK_VERSION}/libexec/logs"
    mkdir "/usr/local/Cellar/apache-spark/${SPARK_VERSION}/libexec/work"

    "/usr/local/Cellar/apache-spark/${SPARK_VERSION}/libexec/sbin/start-all.sh"



## Running

### Client with local spark

    export SCALA_VERSION="2.11"
    export VERSION="0.1"

    spark-submit \
        --class=Main \
        --master=local \
        --deploy-mode=client \
        --conf="spark.hadoop.fs.s3a.access.key=AKIAJVXR75TBLKUQ5P6Q" \
        --conf="spark.hadoop.fs.s3a.secret.key=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" \
        --repositories="https://s3.amazonaws.com/redshift-maven-repository/release" \
        --packages="com.amazon.redshift:redshift-jdbc42:1.2.15.1025,org.apache.hadoop:hadoop-aws:2.7.6" \
        "./target/scala-${SCALA_VERSION}/analytics_${SCALA_VERSION}-${VERSION}.jar"

### Client with Stand Alone

    spark-submit \
        --class=Main \
        --master=spark://127.0.0.1:7077 \
        --deploy-mode=client \
        --conf="spark.hadoop.fs.s3a.access.key=AKIAJVXR75TBLKUQ5P6Q" \
        --conf="spark.hadoop.fs.s3a.secret.key=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" \
        --repositories="https://s3.amazonaws.com/redshift-maven-repository/release" \
        --packages="com.amazon.redshift:redshift-jdbc42:1.2.15.1025,org.apache.hadoop:hadoop-aws:2.7.6" \
        "./target/scala-${SCALA_VERSION}/analytics_${SCALA_VERSION}-${VERSION}.jar"

### Cluster with Stand Alone

    spark-submit \
        --class=Main \
        --master=spark://127.0.0.1:7077 \
        --deploy-mode=cluster \
        --conf="spark.hadoop.fs.s3a.access.key=AKIAJVXR75TBLKUQ5P6Q" \
        --conf="spark.hadoop.fs.s3a.secret.key=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" \
        --repositories="https://s3.amazonaws.com/redshift-maven-repository/release" \
        --packages="com.amazon.redshift:redshift-jdbc42:1.2.15.1025,org.apache.hadoop:hadoop-aws:2.7.6" \
        "./target/scala-${SCALA_VERSION}/analytics_${SCALA_VERSION}-${VERSION}.jar"


### Spark Shell


    spark-shell \
        --master=spark://127.0.0.1:7077 \
        --deploy-mode=client \
        --conf="spark.hadoop.fs.s3a.access.key=AKIAJVXR75TBLKUQ5P6Q" \
        --conf="spark.hadoop.fs.s3a.secret.key=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" \
        --repositories="https://s3.amazonaws.com/redshift-maven-repository/release" \
        --jars="./target/scala-${SCALA_VERSION}/analytics_${SCALA_VERSION}-${VERSION}.jar" \
        --packages="com.amazon.redshift:redshift-jdbc42:1.2.15.1025,org.apache.hadoop:hadoop-aws:2.7.6"
