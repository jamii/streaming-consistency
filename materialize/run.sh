#!/usr/bin/env bash

set -ue

DATAGEN=$1

THIS_DIR="$(cd "$(dirname "$0")"; pwd -P)"

DATA_DIR=$THIS_DIR/tmp
echo "Data will be stored in $DATA_DIR"
rm -rf $DATA_DIR/*
mkdir -p $DATA_DIR/{config,logs}

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
check_port_is_available "Materialize" 6875

echo "Starting materialized"
materialized \
    --experimental \
    --logical-compaction-window=off \
    --data-directory ./tmp/ \
    -w 4 \
    > $DATA_DIR/logs/materialized 2>&1 &
wait_for_port "materialized" 6875

echo "Feeding inputs"
$DATAGEN | cut -d'|' -f2 >> $DATA_DIR/transactions

echo "Creating views"
touch "$DATA_DIR/transactions"
# have to make source separately to get the absolute path
psql postgres://materialize@localhost:6875/materialize -c "CREATE SOURCE transactions_source FROM FILE '$DATA_DIR/transactions' FORMAT BYTES;"
psql postgres://materialize@localhost:6875/materialize -f ./views.sql
   
echo "Watching outputs"
watch_view() { 
     unbuffer psql postgres://materialize@localhost:6875/materialize -c "COPY (TAIL $1 AS OF 0) TO STDOUT" > $DATA_DIR/$1 &
}
watch_view accepted_transactions
watch_view outer_join
watch_view credits
watch_view debits
watch_view balance
watch_view total

echo "All systems go. Hit ctrl-c when you're ready to shut everything down."
read -r -d '' _