#!/bin/sh
echo "Hello"
echo "echo {SPARK_HOME}:" ${SPARK_HOME}

echo "start SPARK standalone local"
echo "now go check localhost:8080 for Spark UI"
${SPARK_HOME}/sbin/start-all.sh


# ${SPARK_HOME}/sbin/stop-all.sh
