# Dockerfile to execute sh file and run spark jobs

FROM sdldevelopers/awstools
VOLUME /src
WORKDIR /src

RUN  apt-get update \
  && apt-get install -y wget \
  && rm -rf /var/lib/apt/lists/*

COPY . /src/

CMD ./emr-load-dce-jobs.sh

