FROM docker.elastic.co/kibana/kibana:7.17.19

RUN mkdir -p /tmp/sample/
# Copy default dashboards
COPY ./docker/kibana/exports/ /tmp/sample/
# Copy custom kibana.yml file
COPY ./docker/kibana/kibana.yml /usr/share/kibana/config/
# Copy initialization script
COPY ./docker/kibana/init-script.sh /tmp/sample/init-script.sh
USER root
# give access to files
RUN chmod -R 777 /tmp/sample/*
# Run 
CMD ["/bin/bash", "-c", "kibana-docker --allow-root & /tmp/sample/init-script.sh"]

