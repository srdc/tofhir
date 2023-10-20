FROM openjdk:11-jre-slim

ENV TOFHIR_HOME /usr/local/tofhir
RUN mkdir -p "$TOFHIR_HOME"
WORKDIR $TOFHIR_HOME

COPY ./tofhir-server/target/tofhir-server-standalone.jar .
COPY ./docker/server/docker-entrypoint.sh .
RUN chmod +x docker-entrypoint.sh

ENTRYPOINT ["./docker-entrypoint.sh"]
