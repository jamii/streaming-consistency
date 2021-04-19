#!/usr/bin/env bash

set -ue

DATAGEN=$1

THIS_DIR="$(cd "$(dirname "$0")"; pwd -P)"

DATA_DIR=$THIS_DIR/tmp
echo "Data will be stored in $DATA_DIR"
rm -rf $DATA_DIR/*
mkdir -p $DATA_DIR/{config,logs}
mkdir -p $DATA_DIR/logs/flink

KAFKA_DIR=$(dirname $(which kafka-server-start.sh))/..

FLINK_DIR=$(dirname $(which flink))/../opt/flink/

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
check_port_is_available 'Flink JobManager' 6123

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
create_topic() {
    kafka-topics.sh --create \
        --bootstrap-server localhost:9092 \
        --replication-factor 1 \
        --partitions 1 \
        --config retention.ms=-1 \
        --topic "$1"
}
create_topic transactions

echo "Starting flink"
$FLINK_DIR/bin/start-cluster.sh
#wait_for_port "flink" 6123

echo "Compiling"
mvn package

echo "Starting demo"
flink run --detached ./target/demo-1.0.0.jar

echo "Feeding inputs"
$DATAGEN | kafka-console-producer.sh \
    --broker-list localhost:9092 \
    --topic transactions \
    --property "key.separator=|" \
    --property "parse.key=true" \
    > /dev/null &
   
echo "Watching outputs"
watch_topic() { 
    kafka-console-consumer.sh \
        --bootstrap-server localhost:9092 \
        --topic "$1" \
        --from-beginning \
        --formatter kafka.tools.DefaultMessageFormatter \
        --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
        --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
        > "./tmp/$1" &
}
watch_topic transactions
    
echo "All systems go. Hit ctrl-c when you're ready to shut everything down."
read -r -d '' _