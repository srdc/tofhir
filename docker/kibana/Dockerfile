FROM docker.elastic.co/kibana/kibana:7.15.2

RUN mkdir -p /tmp/sample/
# Copy default dashboards
COPY ./exports/ /tmp/sample/
# Copy ignore_above template
#COPY ./exports/index_template.json /tmp/sample/index_template.json
# Copy initialization script
COPY init-script.sh /tmp/sample/init-script.sh
USER root
# give access to files
RUN chmod -R 777 /tmp/sample/*
# Run 
CMD ["/bin/bash", "-c", "kibana --allow-root & /tmp/sample/init-script.sh"]

