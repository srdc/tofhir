# Execute one of the following commands from the project.root.directory (../../)
# Do not forget to use the correct version while tagging.

docker build -f docker/engine/Dockerfile-addJar -t srdc/tofhir-engine:latest .
docker build -f docker/engine/Dockerfile-buildJar -t srdc/tofhir-engine:latest .
