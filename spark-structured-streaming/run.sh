#!/usr/bin/env bash

set -ue

# cleanup processes on exit
echo $$ > /sys/fs/cgroup/cpu/jamii-consistency-demo/tasks
cleanup() {
    echo "Cleaning up"
    for pid in $(< /sys/fs/cgroup/cpu/jamii-consistency-demo/tasks) 
    do
        if [ $pid -ne $$ ]
        then
            kill -9 $pid 2> /dev/null || true
        fi
    done
    echo "Done"
}
trap cleanup EXIT

THIS_DIR="$(cd "$(dirname "$0")"; pwd -P)"

DATA_DIR=$THIS_DIR/tmp
echo "Data will be stored in $DATA_DIR"
rm -rf $DATA_DIR/*
mkdir -p $DATA_DIR/logs

SPARK_DIR=$(dirname $(which spark-submit))/../lib/spark-2.4.4-bin-without-hadoop/

# # This doesn't work - it ignores the properties file and tries to write to $SPARK_DIR/logs anyway
#
# cat $SPARK_DIR/conf/spark-defaults.conf.template > $DATA_DIR/config/spark.conf
# cat >> $DATA_DIR/config/spark.conf <<EOF
# spark.driver.log.dfsDir $DATA_DIR/logs
# EOF
# 
# echo "Starting spark"
# $SPARK_DIR/sbin/start-master.sh --properties-file $DATA_DIR/config/spark.conf

# # This also doesn't work, gets overridden by conf/spark-env.sh
#
# export SPARK_HOME=$DATA_DIR/spark

mkdir $DATA_DIR/spark
cp -ar $SPARK_DIR/* $DATA_DIR/spark
chmod -R a+w $DATA_DIR/spark
cat >> $DATA_DIR/spark/conf/spark-env.sh <<EOF
export SPARK_HOME=$DATA_DIR/spark
EOF

echo "Starting spark"
$DATA_DIR/spark/sbin/start-master.sh

echo "Compiling"
mvn package

echo "Submitting demo"
$DATA_DIR/spark/bin/spark-submit \
  --class "Demo" \
  --master local[4] \
  target/demo-1.0.jar