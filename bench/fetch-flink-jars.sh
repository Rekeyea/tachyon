#!/usr/bin/env bash
# Jars que el contenedor de Flink necesita para leer Kafka y escribir Paimon.
set -euo pipefail
cd "$(dirname "$0")/flink" && mkdir -p lib && cd lib
M=https://repo1.maven.org/maven2
curl -sfLO "$M/org/apache/paimon/paimon-flink-2.2/1.4.2/paimon-flink-2.2-1.4.2.jar"
curl -sfLO "$M/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar"
curl -sfLO "$M/org/apache/flink/flink-sql-connector-aws-kinesis-streams/6.0.1-2.0/flink-sql-connector-aws-kinesis-streams-6.0.1-2.0.jar"
curl -sfLO "$M/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar"
ls -1
