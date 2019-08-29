#### Build #####################################################################
FROM adoptopenjdk/openjdk11:x86_64-ubuntu-jdk-11.0.2.9 AS build-image

RUN mkdir -p /app
WORKDIR /app

COPY gradlew .
COPY gradle ./gradle
# to cache latest gradlew
RUN ./gradlew --version

COPY build.gradle .
COPY src ./src
RUN ./gradlew assemble

#### Runtime ###################################################################
FROM adoptopenjdk/openjdk11:x86_64-ubuntu-jre-11.0.2.9
MAINTAINER Minero Aoki <minero-aoki@cookpad.com>

RUN mkdir -p /app
WORKDIR /app

COPY config.docker/ config/
COPY --from=build-image /app/build/libs/bricolage-streaming-preprocessor-LATEST-boot.jar ./bricolage-streaming-preprocessor.jar

CMD ["java", "-Dlogging.config=config/logback.xml", "-jar", "bricolage-streaming-preprocessor.jar"]
