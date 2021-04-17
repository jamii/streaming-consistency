#! /usr/bin/env bash

cat ksqldb/original-results/total | cut -d':' -f 3 | cut -d '}' -f 1 > ksqldb-original-total
cat ksqldb/simplified-results/total | cut -d':' -f 3 | cut -d '}' -f 1 > ksqldb-simplified-total
grep insert flink-table/original-results/total | cut -d' ' -f 2 > flink-original-total
grep insert flink-table/simplified-results/total | cut -d' ' -f 2 > flink-simplified-total
./graph.r
mv ./*.png ~/blog/content/writing/internal-consistency-in-streaming-systems/