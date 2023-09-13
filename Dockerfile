FROM gradle:8.3-jdk17 AS builder
ENV GRADLE_OPTS="-Dorg.gradle.daemon=false"

WORKDIR /app
COPY gradle.properties *.gradle.kts ./
RUN --mount=type=cache,target=/root/.gradle,sharing=locked \
  gradle dependencies

COPY . .
RUN gradle assembleDist
RUN set -eux; \
    cd ./build/distributions/; \
    tar -xvf ./alluvial-0.1.tar

FROM eclipse-temurin:17-jre-jammy
LABEL maintainer="Teko's DataOps Team <dataops@teko.vn>"

COPY --from=builder --link \
    /app/build/distributions/alluvial-0.1/ /opt/alluvial/
ENV PATH=/opt/alluvial/bin:$PATH
