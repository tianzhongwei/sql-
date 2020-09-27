#!/usr/bin/env bash
download[doc://15988 predict3.py]
{
    veh=$1
    vin_tail=$2
    #veh=BYD7003BEV
    #vin_tail=9
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
    ./predict3.py ${veh} ${vin_tail}
}