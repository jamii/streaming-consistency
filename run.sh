#!/usr/bin/env bash

set -ue

THIS_DIR="$(cd "$(dirname "$0")"; pwd -P)"

mkdir -p $1
DATA_DIR="$(cd $1; pwd)"
echo "Data will be stored in $DATA_DIR"
rm -rf $DATA_DIR/*
mkdir -p $DATA_DIR/{config,logs}

KAFKA_DIR=$(dirname $(which kafka-server-start.sh))/..

check_port_is_available() {
    local name="$1"
    local port="$2"
    true &>/dev/null </dev/tcp/127.0.0.1/$port && echo "Something (probably $name) is already running on port $port. Please kill it and try again." && exit 1 || echo "$port is available for $name"
}

wait_for_port() {
    local name="$1"
    local port="$2"
    echo "Waiting for $name (check $DATA_DIR/logs/$name)"
    while ! $(true &>/dev/null </dev/tcp/127.0.0.1/$port)
    do
        echo -n "."
        sleep 0.1
    done
    echo
}

echo "Checking ports"
check_port_is_available "Kafka" 9092
check_port_is_available "Confluent Control Center" 9021
check_port_is_available "Zookeeper" 2181

# TODO cleanup cgroup on exit
# cleanup() {}
# trap cleanup EXIT

echo "Starting zookeeper"
cat > $DATA_DIR/config/zookeeper.properties <<EOF
dataDir=$DATA_DIR/zookeeper
clientPort=2181
maxClientCnxns=0
admin.enableServer=false
EOF
zookeeper-server-start.sh $DATA_DIR/config/zookeeper.properties >$DATA_DIR/logs/zookeeper &
wait_for_port "zookeeper" 2181

echo "Starting kafka"
cat $KAFKA_DIR/config/server.properties > $DATA_DIR/config/server.properties
cat >> $DATA_DIR/config/server.properties <<EOF
# without these, kafka would need to have $HOSTNAME in /etc/hosts
listeners=PLAINTEXT://localhost:9092
advertised.listeners=PLAINTEXT://localhost:9092
log.dirs=$DATA_DIR/kafka-logs
EOF
kafka-server-start.sh $DATA_DIR/config/server.properties >$DATA_DIR/logs/kafka &
wait_for_port "kafka" 9092

echo "Creating topic"
kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092