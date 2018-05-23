# dce_spark_jobs

This is the repo for spark jobs to read all the DICE data

Step1: Upload the jobname.py to S3 bucket
Step2: Upload helper JAR files and Classfiles to bucket
Step3: Upload config file to bucket
Step4: Ensure that EMR cluster is running
Step5: Execute spark job in cluster by adding it to cluster using:
    aws emr add - steps - -region region - -cluster - id clusterid - -steps Type = Spark, Name = "appname", ActionOnFailure = CONTINUE, Args = [--packages, s3: // helperclassfile, --jars, s3: // helper.jars, s3: // bucket / jobname.py]
Step6: Check job execution logs on console / sshing into cluster
