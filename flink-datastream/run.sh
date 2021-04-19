#!/usr/bin/env bash

set -ue

DATAGEN=$1

THIS_DIR="$(cd "$(dirname "$0")"; pwd -P)"

DATA_DIR=$THIS_DIR/tmp
echo "Data will be stored in $DATA_DIR"
rm -rf $DATA_DIR/*
mkdir -p $DATA_DIR/{config,logs}
mkdir -p $DATA_DIR/logs/flink

FLINK_DIR=$(dirname $(which flink))/../opt/flink/

check_port_is_available() {
    local name="$1"
    local port="$2"
    true &>/dev/null </dev/tcp/127.0.0.1/$port && echo "Something (probably $name) is already running on port $port. Please kill it and try again." && exit 1 || echo "$port is available for $name"
}

echo "Checking ports"
check_port_is_available 'Flink JobManager' 6123

echo "Starting flink"
$FLINK_DIR/bin/start-cluster.sh
#wait_for_port "flink" 6123

echo "Compiling"
mvn package

echo "Feeding inputs"
mkdir -p $DATA_DIR/transactions/
$DATAGEN | cut -d'|' -f2 > $DATA_DIR/transactions/1

echo "Starting demo"
flink run --detached ./target/demo-1.0.0.jar
    
echo "All systems go. Hit ctrl-c when you're ready to shut everything down."
read -r -d '' _