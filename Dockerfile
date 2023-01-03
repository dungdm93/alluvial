FROM gradle:7.6-jdk17 AS builder
ENV GRADLE_OPTS="-Dorg.gradle.daemon=false"

WORKDIR /app
COPY gradle.properties *.gradle.kts ./
RUN gradle dependencies

COPY ./src ./src
RUN gradle assembleDist
RUN set -eux; \
    cd ./build/distributions/; \
    tar -xvf ./alluvial-0.1.tar

FROM eclipse-temurin:17-jre-jammy
LABEL maintainer="Teko's DataOps Team <dataops@teko.vn>"

COPY --from=builder /app/build/distributions/alluvial-0.1/ /opt/alluvial/
ENV PATH=/opt/alluvial/bin:$PATH
