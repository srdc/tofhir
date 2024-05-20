#!/bin/bash

# Wait for Elasticsearch to start up before doing anything.
until curl -s http://elasticsearch:9200/_cat/health -o /dev/null; do
    echo Waiting for Elasticsearch...
    sleep 5
done

# Send a request to save ignore_above template in Elasticsearch
# This index template should be established prior to importing the Saved Objects to ensure its configurations are applied to new indexes correctly.
curl -X PUT "http://elasticsearch:9200/_template/ignore_above" -H 'Content-Type: application/json' -d @/tmp/sample/index_template.json

# Function to check if Kibana is ready
check_kibana() {
    # Make a request to Kibana's status API and check the response
    status=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:5601/api/status)
    # If the status is 200 (OK), Kibana is ready
    if [ "$status" -eq 200 ]; then
        return 0
    else
        return 1
    fi
}

# Wait for Kibana to be ready
until check_kibana; do
    echo "Waiting for Kibana..."
    sleep 5
done

# Import default dashboards using the Saved Objects API
curl -X POST "http://localhost:5601/api/saved_objects/_import?overwrite=true" -H "kbn-xsrf: true" -H 'Content-Type: multipart/form-data' --form file=@/tmp/sample/dashboards.ndjson
# Disable telemetry i.e. Help us improve the Elastic Stack notification
curl -X POST "http://localhost:5601/api/telemetry/v2/optIn" -H "kbn-xsrf: true" -H 'Content-Type: application/json' -d '{"enabled":false}'
# Send a dummy data entry to Elasticsearch to initialize the visualizations of "Executions Dashboard".
# This ensures that the dashboard can refresh.
# If no initial data is sent, the dashboard will not initialize properly due to the absence of the "executionId" field.
# This field is crucial for the dashboard's initialization and subsequent refreshing to function correctly.
# Get the current date in the format YYYY.MM.DD
current_date=$(date +'%Y.%m.%d')
# Construct the index name with the current date
index_name="fluentd-$current_date"
# Get the current date and time in ISO 8601 format
current_timestamp=$(date -u +"%Y-%m-%dT%H:%M:%S.%N%:z")
# Construct the JSON data with an empty executionId and the current timestamp
json_data="{\"executionId\": \"\", \"@timestamp\": \"$current_timestamp\"}"
# Send dummy data to Elasticsearch with the dynamically generated index name and JSON data
curl -X POST "http://elasticsearch:9200/$index_name/_doc" -H "Content-Type: application/json" -d "$json_data"

echo $'\nInitialization complete. Keeping container running for Kibana...'
tail -f /dev/null