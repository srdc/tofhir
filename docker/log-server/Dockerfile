FROM openjdk:11-jre-slim

# Create folders
RUN mkdir -p /tofhir
WORKDIR /tofhir

# Add Files
COPY ./tofhir-log-server/target/tofhir-log-server-standalone.jar .
COPY ./docker/log-server/docker-entrypoint.sh .
COPY ./tofhir-log-server/src/main/resources/logback.xml .

RUN chmod +x /tofhir/docker-entrypoint.sh

ENTRYPOINT ["/tofhir/docker-entrypoint.sh"]
