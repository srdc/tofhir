#!/usr/bin/env bash

JAVA_CMD="java -Xms256m -Xmx3g -jar "

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

# Delay the execution for this amount of seconds
if [ ! -z "$DELAY_EXECUTION" ]; then
  sleep $DELAY_EXECUTION
fi

# Finally, tell which jar to run
JAVA_CMD+="tofhir-log-server-standalone.jar"

eval $JAVA_CMD "$@"
