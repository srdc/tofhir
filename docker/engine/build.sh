# Execute one of the following commands from the project.root.directory (../../)

docker build -f docker/engine/Dockerfile-addJar -t srdc/tofhir:latest -t srdc/tofhir:1.0 .
docker build -f docker/engine/Dockerfile-buildJar -t srdc/tofhir:latest -t srdc/tofhir:1.0 .
