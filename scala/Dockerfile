# Dockerfile to execute sh file and run spark jobs

FROM sdldevelopers/awstools
VOLUME /src
WORKDIR /src

RUN  apt-get update \
  && apt-get install -y wget \
  && rm -rf /var/lib/apt/lists/*

RUN wget -O "consul-template.zip" https://releases.hashicorp.com/consul-template/0.19.5/consul-template_0.19.5_linux_386.zip && \
    unzip -o "consul-template.zip" -d /usr/local/bin/ && \
    rm -f "consul-template.zip"

COPY . /src/
CMD mv /src/static/* ./

CMD ./emr-load-dce-jobs.sh

