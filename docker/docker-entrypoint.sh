#!/usr/bin/env bash

JAVA_CMD="java -Xms256m -Xmx3g -jar "

# Configure application.conf path
if [ ! -z "$APP_CONF_FILE" ]; then
  JAVA_CMD+="-Dconfig.file=$APP_CONF_FILE "
fi

# Configure Spark
if [ ! -z "$SPARK_APPNAME" ]; then
  JAVA_CMD+="-Dspark.app-name=$SPARK_APPNAME "
fi
if [ ! -z "$SPARK_MASTER" ]; then
  JAVA_CMD+="-Dspark.master=$SPARK_MASTER "
fi

# Configure toFHIR mapping-related paths
if [ ! -z "$CONTEXT_PATH" ]; then
  JAVA_CMD+="-Dtofhir.context-path=$CONTEXT_PATH "
fi
if [ ! -z "$MAPPINGS_FOLDER" ]; then
  JAVA_CMD+="-Dtofhir.mappings.repository.folder-path=$MAPPINGS_FOLDER "
fi
if [ ! -z "$SCHEMAS_FOLDER" ]; then
  JAVA_CMD+="-Dtofhir.mappings.schemas.repository.folder-path=$SCHEMAS_FOLDER "
fi
if [ ! -z "$MAPPING_JOB" ]; then
  JAVA_CMD+="-Dtofhir.mapping-job.file-path=$MAPPING_JOB "
fi
if [ ! -z "$FHIR_BATCH_SIZE" ]; then
  JAVA_CMD+="-Dtofhir.fhir-writer.batch-group-size=$FHIR_BATCH_SIZE "
fi
if [ ! -z "$DB_PATH" ]; then
  JAVA_CMD+="-Dtofhir.db=$DB_PATH "
fi

# Delay the execution for this amount of seconds
if [ ! -z "$DELAY_EXECUTION" ]; then
  sleep $DELAY_EXECUTION
fi

# Finally, tell which jar to run
JAVA_CMD+="tofhir-standalone.jar"

eval $JAVA_CMD "$@"
