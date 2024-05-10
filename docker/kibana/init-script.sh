#!/bin/bash

# Wait for Elasticsearch to start up before doing anything.
until curl -s http://elasticsearch:9200/_cat/health -o /dev/null; do
    echo Waiting for Elasticsearch...
    sleep 5
done

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
# Send a request to save ignore_above template in Elasticsearch
curl -X PUT "http://elasticsearch:9200/_template/ignore_above" -H 'Content-Type: application/json' -d @/tmp/sample/index_template.json

echo $'\nInitialization complete. Keeping container running for Kibana...'
tail -f /dev/null