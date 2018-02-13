#!/bin/sh

gradle build &&
LOGBACK_CONFIG=config/logback.xml SPRING_PROFILES_ACTIVE=development $(dirname $0)/bricolage-streaming-preprocessor "$@"
