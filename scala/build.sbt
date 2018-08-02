resolvers += "redshift" at "https://s3.amazonaws.com/redshift-maven-repository/release"

name := "analytics"
version := "0.1"
scalaVersion := "2.11.12"

// Hadoop AWS (Need to match version Spark is compiled with)
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.7.6"

// Spark
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.1"

// ScalaTest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"

// RedShift (includes AWS SDK - we use this as we need AWS SDK 1.7.4 for Hadoop-AWS)
libraryDependencies += "com.amazon.redshift" % "redshift-jdbc42" % "1.2.15.1025"

// CLI tools
libraryDependencies += "com.github.scopt" %% "scopt" % "3.7.0"


Test / testOptions += Tests.Argument("-oF")
