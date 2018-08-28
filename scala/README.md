# Read me


## Dependencies

- Scala 1.11
- SBT 1.1
- Java 8
- Spark 2.3


## Java

    brew tap caskroom/versions
    brew cask install java8


## JEnv

    brew install jenv

### Bash

	echo 'export PATH="$HOME/.jenv/bin:$PATH"' >> ~/.bash_profile
	echo 'eval "$(jenv init -)"' >> ~/.bash_profile

### ZSH

	echo 'export PATH="$HOME/.jenv/bin:$PATH"' >> ~/.zshrc
	echo 'eval "$(jenv init -)"' >> ~/.zshrc

### Adding Java Versions

    jenv add /Library/Java/JavaVirtualMachines/jdk-9.0.1.jdk/Contents/Home/
    jenv add /Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/


## SBT

    brew install sbt


## Spark

    brew install apache-spark


## Packaging JAR file

    sbt package


## Stand Alone


    export SPARK_VERSION="2.3.1"
    export SPARK_MASTER_HOST="127.0.0.1"

### Starting

On OS X you need to enable Remote Login under Sharing in System Preferences

    mkdir "/usr/local/Cellar/apache-spark/${SPARK_VERSION}/libexec/logs"
    mkdir "/usr/local/Cellar/apache-spark/${SPARK_VERSION}/libexec/work"

    "/usr/local/Cellar/apache-spark/${SPARK_VERSION}/libexec/sbin/start-all.sh"

### Stopping

    "/usr/local/Cellar/apache-spark/${SPARK_VERSION}/libexec/sbin/stop-all.sh"


### Performance Tuning

    "/usr/local/Cellar/apache-spark/${SPARK_VERSION}/libexec/conf/spark-env.sh"

    SPARK_EXECUTOR_MEMORY=2048M

    SPARK_WORKER_INSTANCES=1
    SPARK_WORKER_MEMORY=6G
    SPARK_WORKER_CORES=4



## Spark Submit / Running

### Local

    export SCALA_VERSION="2.11"
    export HADOOP_VERSION="2.7.6"
    export VERSION="0.1"

    export AWS_ACCESS_KEY_ID="AKIAJVXR75TBLKUQ5P6Q"
    export AWS_SECRET_ACCESS_KEY="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"

    export JDBC_URL="jdbc:postgresql://qa-redshift-cluster.camxxuuvbchc.eu-west-1.redshift.amazonaws.com/redshift"
    export JDBC_USERNAME="saffron"
    export JDBC_PASSWORD="1Nn0v8t3"
    export JDBC_DRIVER="com.amazon.redshift.jdbc.Driver"

    spark-submit \
        --class=Signup \
        --master="local[*]" \
        --deploy-mode=client \
        --conf="spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID}" \
        --conf="spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY}" \
        --conf="spark.jdbc.url=${JDBC_URL}" \
        --conf="spark.jdbc.username=${JDBC_USERNAME}" \
        --conf="spark.jdbc.password=${JDBC_PASSWORD}" \
        --conf="spark.jdbc.driver=${JDBC_DRIVER}" \
        --repositories="https://s3.amazonaws.com/redshift-maven-repository/release" \
        --packages="com.amazon.redshift:redshift-jdbc42:1.2.15.1025,org.apache.hadoop:hadoop-aws:${HADOOP_VERSION},com.github.scopt:scopt_2.11:3.7.0,com.databricks:spark-redshift_2.11:3.0.0-preview1" \
        "./target/scala-${SCALA_VERSION}/analytics_${SCALA_VERSION}-${VERSION}.jar" \
        --path="path to s3 bucket s3a://"


### Master

    spark-submit \
        --class=Main \
        --master=spark://127.0.0.1:7077 \
        --deploy-mode=client \
        --conf="spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID}" \
        --conf="spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY}" \
        --conf="spark.jdbc.url=${JDBC_URL}" \
        --conf="spark.jdbc.username=${JDBC_USERNAME}" \
        --conf="spark.jdbc.password=${JDBC_PASSWORD}" \
        --conf="spark.jdbc.driver=${JDBC_DRIVER}" \
        --repositories="https://s3.amazonaws.com/redshift-maven-repository/release" \
        --packages="com.amazon.redshift:redshift-jdbc42:1.2.15.1025,org.apache.hadoop:hadoop-aws:${HADOOP_VERSION},com.github.scopt:scopt_2.11:3.7.0,com.databricks:spark-redshift_2.11:3.0.0-preview1" \
        "./target/scala-${SCALA_VERSION}/analytics_${SCALA_VERSION}-${VERSION}.jar"



## Spark Shell / REPL

    spark-shell \
        --master=spark://127.0.0.1:7077 \
        --deploy-mode=client \
        --conf="spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID}" \
        --conf="spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY}" \
        --conf="spark.jdbc.url=${JDBC_URL}" \
        --conf="spark.jdbc.username=${JDBC_USERNAME}" \
        --conf="spark.jdbc.password=${JDBC_PASSWORD}" \
        --conf="spark.jdbc.driver=${JDBC_DRIVER}" \
        --repositories="https://s3.amazonaws.com/redshift-maven-repository/release" \
        --jars="./target/scala-${SCALA_VERSION}/analytics_${SCALA_VERSION}-${VERSION}.jar" \
        --packages="com.amazon.redshift:redshift-jdbc42:1.2.15.1025,org.apache.hadoop:hadoop-aws:${HADOOP_VERSION},com.github.scopt:scopt_2.11:3.7.0,com.databricks:spark-redshift_2.11:3.0.0-preview1"



## Spark SQL

    spark-sql \
        --master=spark://127.0.0.1:7077 \
        --deploy-mode=client \
        --conf="spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID}" \
        --conf="spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY}" \
        --conf="spark.jdbc.url=${JDBC_URL}" \
        --conf="spark.jdbc.username=${JDBC_USERNAME}" \
        --conf="spark.jdbc.password=${JDBC_PASSWORD}" \
        --conf="spark.jdbc.driver=${JDBC_DRIVER}" \
        --repositories="https://s3.amazonaws.com/redshift-maven-repository/release" \
        --jars="./target/scala-${SCALA_VERSION}/analytics_${SCALA_VERSION}-${VERSION}.jar" \
        --packages="com.amazon.redshift:redshift-jdbc42:1.2.15.1025,org.apache.hadoop:hadoop-aws:${HADOOP_VERSION},com.github.scopt:scopt_2.11:3.7.0,com.databricks:spark-redshift_2.11:3.0.0-preview1"

