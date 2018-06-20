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

// SBT
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"

// RedShift (with AWS SDK we need 1.7.4 for Hadoop-aws)
libraryDependencies += "com.amazon.redshift" % "redshift-jdbc42" % "1.2.15.1025"
