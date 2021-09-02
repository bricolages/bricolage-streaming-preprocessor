#### Build #####################################################################
FROM amazoncorretto:11 AS build-image

WORKDIR /app

COPY gradlew .
COPY gradle ./gradle
# to cache latest gradlew
RUN ./gradlew --version

COPY build.gradle .
COPY src ./src
RUN ./gradlew assemble

#### Runtime ###################################################################
FROM amazoncorretto:11-alpine

WORKDIR /app

COPY config.docker/ config/
COPY --from=build-image /app/build/libs/bricolage-streaming-preprocessor-LATEST-boot.jar ./bricolage-streaming-preprocessor.jar

CMD ["java", "-Dlogging.config=config/logback.xml", "-jar", "bricolage-streaming-preprocessor.jar"]
