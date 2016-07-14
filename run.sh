#!/bin/sh

gradle build &&
java -Dlogging.config=config/logback.xml -jar build/libs/bricolage-streaming-preprocessor.jar "$@"
