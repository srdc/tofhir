#!/bin/bash

JAVA_CMD="java -jar "

# Configure application.conf path
if [ ! -z "$APP_CONF_FILE" ]; then
    JAVA_CMD+="-Dconfig.file=$APP_CONF_FILE "
fi

if [ ! -z "$FHIR_REPO_URL" ]; then
    JAVA_CMD+="-Dfhir.definitions-fhir-endpoint=$FHIR_REPO_URL "
fi

# Finally, tell which jar to run
JAVA_CMD+="/tofhir/tofhir-server-standalone.jar"

echo "Running command: $JAVA_CMD"

eval $JAVA_CMD
