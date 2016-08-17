#!/bin/sh

gradle build &&
LOGBACK_CONFIG=config/logback.xml $(dirname $0)/bricolage-streaming-preprocessor "$@"
