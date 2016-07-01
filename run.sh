#!/bin/sh

gradle build &&
java -Dlogging.config=./logback-dev.xml -jar build/libs/bricolage-streaming-preprocessor.jar "$@"
