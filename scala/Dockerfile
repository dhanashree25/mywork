# Dockerfile to build scala, sbt and jar file

FROM openjdk:8
RUN wget -O- "http://downloads.lightbend.com/scala/2.11.12/scala-2.11.12.tgz" \
    | tar xzf - -C /usr/local --strip-components=1

Run wget -O- "https://github.com/sbt/sbt/releases/download/v1.1.6/sbt-1.1.6.tgz" \
    |  tar xzf - -C /usr/local --strip-components=1 \
    && sbt exit

VOLUME /src
WORKDIR /src

COPY . /src/

ENTRYPOINT ["sbt"]
CMD [ "package"]

