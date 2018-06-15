# dce_spark_jobs

This is the repo for spark jobs to read all the DICE data

Step1: Upload the jobname.py to S3 bucket
Step2: Upload helper JAR files and Classfiles to bucket
Step3: Upload config file to bucket
Step4: Ensure that EMR cluster is running
Step5: Execute spark job in cluster by adding it to cluster using:
    aws emr add - steps - -region region - -cluster - id clusterid - -steps Type = Spark, Name = "appname", ActionOnFailure = CONTINUE, Args = [--packages, s3: // helperclassfile, --jars, s3: // helper.jars, s3: // bucket / jobname.py]
Step6: Check job execution logs on console / sshing into cluster
eg.
 bin/spark-submit --jars /Users/dhanashree.deval/work/dce_spark_jobs/minimal-json-0.9.5-SNAPSHOT.jar,/Users/dhanashree.deval/work/dce_spark_jobs/RedshiftJDBC42-1.2.12.1017.jar,/Users/dhanashree.deval/work/dce_spark_jobs/spark-redshift_2.11-3.0.0-preview1.jar,/Users/dhanashree.deval/work/dce_spark_jobs/postgresql-42.2.2.jar, --packages com.amazonaws:aws-java-sdk-s3:1.11.21,org.apache.hadoop:hadoop-aws:2.7.3,com.databricks:spark-redshift_2.11:3.0.0-preview1,com.databricks:spark-avro_2.11:3.2.0 /Users/dhanashree.deval/work/dce_spark_jobs/user_logins.py 
