#!/usr/bin/env bash
download[doc://15977 train2.py]
{
    veh=$1
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
    --name "tianzw_vol_fading_second_versions" \
    ./train2.py ${veh}
}