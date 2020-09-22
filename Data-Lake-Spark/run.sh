#!/bin/sh
echo "Hello"
echo "echo {SPARK_HOME}:" ${SPARK_HOME}

echo "start SPARK standalone local"
${SPARK_HOME}/sbin/start-all.sh