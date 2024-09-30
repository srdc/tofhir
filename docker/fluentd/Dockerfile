#Specifies the base image for building the Fluentd Docker image
FROM fluent/fluentd:v1.17-1
#Switches to the root user account to perform package installations and configurations.
USER root
#Installs necessary packages and dependencies using the Alpine package manager (apk).
RUN apk add --no-cache --update --virtual .build-deps \
    sudo build-base ruby-dev \
    && sudo gem install faraday \
    && sudo gem install faraday-net_http \
    && sudo gem install fluent-plugin-elasticsearch \
    && sudo gem sources --clear-all \
    && apk del .build-deps \
    && rm -rf /tmp/* /var/tmp/* /usr/lib/ruby/gems/*/cache/*.gem
# Switches back to the fluent user account to run Fluentd.
USER fluent
# Copies Fluentd configuration file into the image
COPY ./docker/fluentd/conf/fluent.conf /fluentd/etc/fluent.conf