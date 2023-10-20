FROM openjdk:11-jre-slim

ENV TOFHIR_HOME /usr/local/tofhir
RUN mkdir -p "$TOFHIR_HOME"
WORKDIR $TOFHIR_HOME

COPY ./tofhir-log-server/target/tofhir-log-server-standalone.jar .
COPY ./docker/log-server/docker-entrypoint.sh .
RUN chmod +x docker-entrypoint.sh

ENTRYPOINT ["./docker-entrypoint.sh"]
