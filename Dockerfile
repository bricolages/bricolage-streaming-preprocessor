#### Build #####################################################################
FROM eclipse-temurin:11-jdk-focal AS build-image

WORKDIR /app

COPY gradlew .
COPY gradle ./gradle
# to cache latest gradlew
RUN ./gradlew --version

COPY build.gradle .
COPY src ./src
RUN ./gradlew assemble

#### Runtime ###################################################################
FROM eclipse-temurin:11-jre-focal

WORKDIR /app

COPY config.docker/ config/
COPY --from=build-image /app/build/libs/bricolage-streaming-preprocessor-LATEST-boot.jar ./bricolage-streaming-preprocessor.jar

CMD ["java", "-Dlogging.config=config/logback.xml", "-jar", "bricolage-streaming-preprocessor.jar"]
