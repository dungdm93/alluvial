ARG DEBEZIUM_VERSION=2.3

FROM maven:3.6-jdk-8-alpine AS downloader
SHELL ["/bin/bash", "-c"]
RUN mkdir -p /build/plugins
WORKDIR /build/

COPY ./pom.xml /build/
RUN --mount=type=cache,target=/root/.m2/repository \
    mvn dependency:resolve \
        dependency:copy-dependencies -DoutputDirectory=plugins/confluent-schema-registry/;

FROM debezium/connect:$DEBEZIUM_VERSION AS entrypoint
USER root
RUN set -eux; \
    sed -i '/${ENABLE_APICURIO_CONVERTERS:=false}/i : ${ENABLE_CONFLUENT_CONVERTERS:=false}' /docker-entrypoint.sh; \
    sed -i '/"Apicurio connectors"/i set_connector_additonal_resource_availability $ENABLE_CONFLUENT_CONVERTERS "confluent-schema-registry" "*" "Confluent SchemaRegistry"' /docker-entrypoint.sh;

FROM debezium/connect:$DEBEZIUM_VERSION
COPY --from=downloader --chown=kafka:kafka --link \
    /build/plugins /kafka/external_libs
COPY --from=entrypoint /docker-entrypoint.sh /docker-entrypoint.sh
