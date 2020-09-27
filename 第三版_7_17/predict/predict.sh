#!/usr/bin/env bash

download[doc://16048 predict.py]

{
    veh=$1
    vin_tail=$2
    seg=$3
    name=$4
    queue="product"
}
{
    spark-submit \
    --master yarn \
    --queue ${queue} \
    --deploy-mode cluster \
    --num-executors 50 \
    --driver-memory 8g \
    --executor-memory 4g \
    --executor-cores 4 \
    --conf spark.kryoserializer.buffer.max=512m \
    --name ${name} \
    predict.py ${veh} ${vin_tail} ${seg}
}