FROM eclipse-temurin:11-jre

# We need curl for healthcheck
RUN apt -y update; apt -y install curl

ENV TOFHIR_HOME=/usr/local/tofhir
RUN mkdir -p "$TOFHIR_HOME"
WORKDIR $TOFHIR_HOME

COPY ./tofhir-server/target/tofhir-server-standalone.jar .
COPY ./docker/server/docker-entrypoint.sh .
RUN chmod +x docker-entrypoint.sh

ENTRYPOINT ["./docker-entrypoint.sh"]
