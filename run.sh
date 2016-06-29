#!/bin/sh

gradle xjar &&
java -Dlogback.configurationFile=./logback-dev.xml -jar build/libs/bricolage-streaming-preprocessor.jar "$@"
