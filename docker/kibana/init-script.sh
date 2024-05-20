#!/bin/bash

# Wait for Elasticsearch to start up before doing anything.
until curl -s http://elasticsearch:9200/_cat/health -o /dev/null; do
    echo Waiting for Elasticsearch...
    sleep 5
done

# The following index templates should be established prior to importing the Saved Objects to ensure their configurations are applied to new indexes correctly.

# Send a request to apply the ignore_above template in Elasticsearch.
# This template increases the default maximum length of (256 characters) text types, and thanks to it, we can
# utilize them in dashboard visualizations.
curl -X PUT "http://elasticsearch:9200/_template/ignore_above" -H 'Content-Type: application/json' -d @/tmp/sample/ignore_above_index_template.json
# Certain fields are required for Kibana Dashboards to initialize properly.
# These fields are populated using the index template. Without such an index template, the dashboards cannot be initialized
# properly until the first data entry is sent to Elasticsearch. Since the dashboards are not initialized correctly,
# the refresh functionality does not work, even if new data is sent to Elasticsearch (You have the reload the page).
curl -X PUT "http://elasticsearch:9200/_template/required_fields" -H 'Content-Type: application/json' -d @/tmp/sample/required_fields_index_template.json

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

echo $'\nInitialization complete. Keeping container running for Kibana...'
tail -f /dev/null