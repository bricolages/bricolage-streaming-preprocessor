FROM vixns/java8:latest
MAINTAINER Minero Aoki <minero-aoki@cookpad.com>

VOLUME /tmp
RUN mkdir -p /log

# App
WORKDIR /app
COPY build/libs/bricolage-streaming-preprocessor.jar .
COPY config.docker/ config/

CMD ["java", "-Dlogging.config=config/logback.xml", "-jar", "bricolage-streaming-preprocessor.jar"]
