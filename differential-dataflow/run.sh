#!/usr/bin/env bash

set -ue

DATAGEN=$1

THIS_DIR="$(cd "$(dirname "$0")"; pwd -P)"

DATA_DIR=$THIS_DIR/tmp
echo "Data will be stored in $DATA_DIR"
rm -rf $DATA_DIR/*
mkdir -p $DATA_DIR/{config,logs}

echo "Feeding inputs"
$DATAGEN | cut -d'|' -f2 > $DATA_DIR/transactions

echo "Starting demo"
cargo run --release -- -w 4
    
echo "Done!"