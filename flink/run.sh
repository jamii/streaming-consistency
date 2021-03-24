#!/usr/bin/env bash

set -ue

THIS_DIR="$(cd "$(dirname "$0")"; pwd -P)"

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

echo "Compiling"
mvn package

echo "Running example"
flink run ./target/demo-1.0.0.jar

echo "Watching logs"
tail -F /tmp/flink-logs/*