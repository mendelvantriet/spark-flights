FROM openjdk:8-jdk

ENV APP_HOME=/opt/flights
ENV JAVA_EXTRA_OPTS="-Xms2g -Xmx4g"
ENV SPARK_EXTRA_OPTS="-Dspark.driver.memory=1g -Dspark.executor.memory=1g"

WORKDIR /opt

COPY target/flights-*.jar $APP_HOME/flights.jar
COPY entrypoint.sh /opt/

RUN mkdir -p $APP_HOME && \
    chmod +x /opt/entrypoint.sh

WORKDIR $APP_HOME

ENTRYPOINT ["/opt/entrypoint.sh"]

