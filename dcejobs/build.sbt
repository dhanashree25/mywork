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

// RedShift DataBricks
libraryDependencies += "com.databricks" %% "spark-redshift" % "3.0.0-preview1"

// CLI tools
libraryDependencies += "com.github.scopt" %% "scopt" % "3.7.0"


// Full stack traces
Test / testOptions += Tests.Argument("-oF")

// Fork the test process, this allows us to allocate our own memory and prevents crashing sbt
Test / fork := true

// Disable parallel execution, as it's not possible with Spark
Test / parallelExecution := false

// Increase memory amount to 2 GB
Test / javaOptions ++= Seq("-Xms2048m", "-Xmx2048m")

// Increase max Permanent Generation (PermGen) size which holds loaded classes, interned strings
Test / javaOptions ++= Seq("-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

// Unload Permanent Generation (PermGen) classes, this reclaims memory for tests
Test / javaOptions ++= Seq("-XX:+CMSClassUnloadingEnabled")
