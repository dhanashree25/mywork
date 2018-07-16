# Dockerfile to execute sh file and run spark jobs

from sdldevelopers/awstools

RUN echo "hello"

CMD aws s3 cp --recursive s3://test-dce-spark/emr-load-dce-jobs.sh  .
CMD aws s3 cp --recursive s3://test-dce-spark/analytics*.jar

VOLUME /src
WORKDIR /src

COPY . /src/

CMD ./emr-load-dce-jobs.sh

 #ENTRYPOINT ["sh"]

