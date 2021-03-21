#!/usr/bin/env bash

set -ue

THIS_DIR="$(cd "$(dirname "$0")"; pwd -P)"

DATA_DIR=$THIS_DIR/tmp
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

echo "Creating topics"
kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1 \
    --topic streams-pageview-input
kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1 \
    --topic streams-userprofile-input
kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1 \
    --topic streams-pageviewstats-untyped-output

echo "Compiling"
mvn package

echo "Running streams (check $DATA_DIR/logs/streams)"
java -cp target/demo-1.0-SNAPSHOT-jar-with-dependencies.jar net.scattered_thoughts.streaming_consistency.PageViewUntypedDemo >$DATA_DIR/logs/streams &

produce_pageview() {
    echo -e "$1" | kafka-console-producer.sh --broker-list localhost:9092 --topic streams-pageview-input --property "key.separator=:" --property "parse.key=true" 
}

produce_user() {
    echo -e "$1" | kafka-console-producer.sh --broker-list localhost:9092 --topic streams-user-input --property "key.separator=:" --property "parse.key=true" 
}

echo "Feeding input"
produce_user 'alice:{"region": "narnia", "timestamp": 0}'
produce_user 'bob:{"region": "narnia2", "timestamp": 1000}'
produce_user 'eve:{"region": "narnia", "timestamp": 2000}'
produce_pageview 'alice:{"user": "alice", "page": "foo", "timestamp": 500}'
produce_pageview 'bob:{"user": "bob", "page": "foo", "timestamp": 3000}'
produce_pageview 'eve:{"user": "eve", "page": "foo", "timestamp": 4000}'
produce_pageview 'eve:{"user": "eve", "page": "foo", "timestamp": 4001}'
produce_pageview 'eve:{"user": "eve", "page": "foo", "timestamp": 4002}'

echo "Reading output"
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic streams-pageviewstats-untyped-output \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
