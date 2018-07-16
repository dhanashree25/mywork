# Dockerfile to execute sh file and run spark jobs

FROM sdldevelopers/awstools

VOLUME /src
WORKDIR /src

COPY . /src/

CMD ./emr-load-dce-jobs.sh

