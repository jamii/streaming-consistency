#!/usr/bin/env bash

set -ue

THIS_DIR="$(cd "$(dirname "$0")"; pwd -P)"

DATA_DIR=$THIS_DIR/tmp
echo "Data will be stored in $DATA_DIR"
rm -rf $DATA_DIR/*

echo "Feeding inputs"
../transactions.py | cut -d'|' -f2 > $DATA_DIR/transactions

echo "Starting demo"
cargo run --release -- -w 4
    
echo "Done!"