FROM openjdk:11-jre-slim

# Create folders
RUN mkdir -p /tofhir
WORKDIR /tofhir

# Add Files
COPY ./tofhir-server/target/tofhir-server-standalone.jar .
COPY ./docker/server/docker-entrypoint.sh .
COPY ./tofhir-server/src/main/resources/logback.xml .

RUN chmod +x /tofhir/docker-entrypoint.sh

ENTRYPOINT ["/tofhir/docker-entrypoint.sh"]
