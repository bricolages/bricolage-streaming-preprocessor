FROM vixns/java8:latest
MAINTAINER Minero Aoki <minero-aoki@cookpad.com>

VOLUME /tmp
RUN mkdir -p /log

# App
WORKDIR /app
RUN mkdir -p build/libs
COPY build/libs/bricolage-streaming-preprocessor.jar build/libs/
COPY config.docker/ config/

# Gradle (flyway)
COPY gradlew build.gradle settings.gradle ./
RUN mv config/gradle.properties .
COPY gradle/ gradle/
RUN mkdir -p src/main/resources/db/migration
COPY src/main/resources/db/migration/ src/main/resources/db/migration/
ENV GRADLE_USER_HOME /app/gradle
# Force to download all dependencies.
# This command must fail because DB container does not exist on build-time; just ignore errors.
RUN sh -c "./gradlew flywayInfo ; true"

CMD ["java", "-Dlogging.config=config/logback.xml", "-jar", "build/libs/bricolage-streaming-preprocessor.jar"]
