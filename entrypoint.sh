#!/bin/bash

set -e

exec java -jar $APP_HOME/flights.jar "$@" $JAVA_EXTRA_OPTS $SPARK_EXTRA_OPTS

