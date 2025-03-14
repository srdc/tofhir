#!/usr/bin/env bash

# Set default Java options
DEFAULT_JAVA_OPTIONS="-Xms256m -Xmx4g"

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
if [ ! -z "$FLUENT_HOST" ]; then
  JAVA_CMD+="-Defk.fluent-host=$FLUENT_HOST "
fi

# Configure Spark
if [ ! -z "$SPARK_APPNAME" ]; then
  JAVA_CMD+="-Dspark.app-name=$SPARK_APPNAME "
fi
if [ ! -z "$SPARK_MASTER" ]; then
  JAVA_CMD+="-Dspark.master=$SPARK_MASTER "
fi
if [ ! -z "$SPARK_UIENABLED" ]; then
  JAVA_CMD+="-Dspark.ui.enabled=$SPARK_UIENABLED "
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

# Configure the FHIR endpoint to which this toFHIR can connect to retrieve resource definitions etc.
if [ ! -z "$FHIR_DEFINITIONS_FHIRVERSION" ]; then
  JAVA_CMD+="-Dfhir.fhir-version=$FHIR_DEFINITIONS_FHIRVERSION "
fi
if [ ! -z "$FHIR_DEFINITIONS_ENDPOINT" ]; then
  JAVA_CMD+="-Dfhir.definitions-fhir-endpoint=$FHIR_DEFINITIONS_ENDPOINT "
fi
if [ ! -z "$FHIR_DEFINITIONS_ROOT_URL_0" ]; then
  JAVA_CMD+="-Dfhir.definitions-root-urls.0=$FHIR_DEFINITIONS_ROOT_URL_0 "
fi
if [ ! -z "$FHIR_DEFINITIONS_ROOT_URL_1" ]; then
  JAVA_CMD+="-Dfhir.definitions-root-urls.1=$FHIR_DEFINITIONS_ROOT_URL_1 "
fi
if [ ! -z "$FHIR_DEFINITIONS_ROOT_URL_2" ]; then
  JAVA_CMD+="-Dfhir.definitions-root-urls.2=$FHIR_DEFINITIONS_ROOT_URL_2 "
fi
if [ ! -z "$FHIR_DEFINITIONS_PROFILES_PATH" ]; then
  JAVA_CMD+="-Dfhir.profiles-path=$FHIR_DEFINITIONS_PROFILES_PATH "
fi
if [ ! -z "$FHIR_DEFINITIONS_VALUESETS_PATH" ]; then
  JAVA_CMD+="-Dfhir.valuesets-path=$FHIR_DEFINITIONS_VALUESETS_PATH "
fi
if [ ! -z "$FHIR_DEFINITIONS_CODESYSTEMS_URL" ]; then
  JAVA_CMD+="-Dfhir.codesystems-path=$FHIR_DEFINITIONS_CODESYSTEMS_URL "
fi

# Configure tofhir-server web server
if [ ! -z "$WEBSERVER_HOST" ]; then
  JAVA_CMD+="-Dwebserver.host=$WEBSERVER_HOST "
fi
if [ ! -z "$WEBSERVER_PORT" ]; then
  JAVA_CMD+="-Dwebserver.port=$WEBSERVER_PORT "
fi
if [ ! -z "$WEBSERVER_BASEURI" ]; then
  JAVA_CMD+="-Dwebserver.base-uri=$WEBSERVER_BASEURI "
fi

# Configure tofhir-redcap
if [ ! -z "$TOFHIR_REDCAP_ENDPOINT" ]; then
  JAVA_CMD+="-Dtofhir-redcap.endpoint=$TOFHIR_REDCAP_ENDPOINT "
fi
if [ ! -z "$TOFHIR_REDCAP_NOTIFICATION_PATH" ]; then
  JAVA_CMD+="-Dtofhir-redcap.paths.notification=$TOFHIR_REDCAP_NOTIFICATION_PATH "
fi
if [ ! -z "$TOFHIR_REDCAP_PROJECTS_PATH" ]; then
  JAVA_CMD+="-Dtofhir-redcap.paths.projects=$TOFHIR_REDCAP_PROJECTS_PATH "
fi
if [ ! -z "$TOFHIR_REDCAP_PROJECT_DATA_PATH" ]; then
  JAVA_CMD+="-Dtofhir-redcap.paths.projectData=$TOFHIR_REDCAP_PROJECT_DATA_PATH "
fi
if [ ! -z "$TOFHIR_REDCAP_RELOAD_PARAMETER" ]; then
  JAVA_CMD+="-Dtofhir-redcap.parameters.reload=$TOFHIR_REDCAP_RELOAD_PARAMETER "
fi

# Delay the execution for this amount of seconds
if [ ! -z "$DELAY_EXECUTION" ]; then
  sleep $DELAY_EXECUTION
fi

# Finally, tell which jar to run
JAVA_CMD+="tofhir-server-standalone.jar"

# Use 'exec' to replace the shell process with the Java process.
# This ensures that the Java application becomes the main process in the container,
# allowing signals such as SIGTERM to be properly handled.
exec $JAVA_CMD "$@"
