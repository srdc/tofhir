#!/usr/bin/env bash

# Set default Java options
DEFAULT_JAVA_OPTIONS="-Xms256m -Xmx3g"

# Use environment variable if provided, otherwise use default value
JAVA_OPTIONS="${JAVA_OPTIONS:-$DEFAULT_JAVA_OPTIONS}"

# Construct JAVA_CMD with Java options
JAVA_CMD="java $JAVA_OPTIONS -jar "

# Configure application.conf path
if [ ! -z "$APP_CONF_FILE" ]; then
  JAVA_CMD+="-Dconfig.file=$APP_CONF_FILE "
fi

# Configure logback configuration file
if [ ! -z "$LOGBACK_CONF_FILE" ]; then
  JAVA_CMD+="-Dlogback.configurationFile=$LOGBACK_CONF_FILE "
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
  JAVA_CMD+="-Dtofhir.mapping-jobs.initial-job-file-path=$MAPPING_JOB "
fi
if [ ! -z "$FHIR_BATCH_SIZE" ]; then
  JAVA_CMD+="-Dtofhir.fhir-writer.batch-group-size=$FHIR_BATCH_SIZE "
fi
if [ ! -z "$DB_PATH" ]; then
  JAVA_CMD+="-Dtofhir.db-path=$DB_PATH "
fi

# Delay the execution for this amount of seconds
if [ ! -z "$DELAY_EXECUTION" ]; then
  sleep $DELAY_EXECUTION
fi

# Finally, tell which jar to run
JAVA_CMD+="tofhir-engine-standalone.jar"

eval $JAVA_CMD "$@"
