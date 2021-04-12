#!/usr/bin/env bash

set -ue

# cleanup processes on exit
cleanup() {
    echo "Cleaning up"
    docker-compose down
    echo "Done"
}
trap cleanup EXIT

THIS_DIR="$(cd "$(dirname "$0")"; pwd -P)"

DATA_DIR=$THIS_DIR/tmp
echo "Data will be stored in $DATA_DIR"
rm -rf $DATA_DIR/*
mkdir -p $DATA_DIR/{config,logs}

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

echo "Starting ksqldb and co"
docker-compose up > $DATA_DIR/logs/ksqldb 2>&1 &
wait_for_port zookeeper 2181
wait_for_port kafka 29092
wait_for_port ksqldb 8088

echo "Waiting until ksqldb is ready for commands"
while ! $(docker-compose exec -T ksqldb-cli ksql http://ksqldb-server:8088 -e 'show topics;' 2> /dev/null | grep -q "default_ksql_processing_log") 
do
    echo -n "."
done
echo

echo "Installing views"
# cant do all the views in one command - produces "Failed to guarantee existence of topic accepted_transactions"
docker-compose exec -T ksqldb-cli ksql http://ksqldb-server:8088 -e "$(cat views1.sql)" > /dev/null 2> /dev/null
docker-compose exec -T ksqldb-cli ksql http://ksqldb-server:8088 -e "$(cat views2.sql)" > /dev/null 2> /dev/null

# TODO this exits, probably same problem as above
echo "Watching outputs"
watch_topic() { 
    COMPOSE_INTERACTIVE_NO_CLI=1 docker-compose exec -T broker kafka-console-consumer \
        --bootstrap-server localhost:29092 \
        --topic "$1" \
        --from-beginning \
        --formatter kafka.tools.DefaultMessageFormatter \
        --property print.timestamp=true \
        --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
        --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
        > "./tmp/$1" &
}
watch_topic transactions
watch_topic accepted_transactions
watch_topic credits
watch_topic debits
watch_topic balance
watch_topic total

echo "Feeding inputs"
../transactions.py | docker-compose exec -T broker kafka-console-producer \
    --broker-list localhost:29092 \
    --topic transactions \
    --property "key.separator=|" \
    --property "parse.key=true" \
    > /dev/null &

echo "All systems go. Hit ctrl-c when you're ready to shut everything down."
read -r -d '' _