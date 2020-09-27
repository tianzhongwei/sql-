#!/usr/bin/env bash
download[doc://15669 train.py]
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
    --conf spark.kryoserializer.buffer.max=256m \
    --name "tianzw_vol_fading_second_versions" \
    ./train.py ${veh}
}