{
    tab_in="nei_temp.tianzw_battery_first_feature_1000"
    tab_out="nei_temp.tianzw_battery_temperture_history_20200605"
}
{
    config="SET mapreduce.job.queuename = product;
            SET mapred.job.name = tianzw_battery_fading;
            set hive.strict.checks.large.query = false;
            set hive.mapred.mode = nonstrict;
            set hive.exec.dynamic.partition.mode=nonstrict;
            "
}
{
    sql_1="
        CREATE TABLE IF NOT EXISTS ${tab_out}
        (
            veh                             STRING          COMMENT '车型名',
            vin                             STRING          COMMENT '车架号',
            year                            STRING          COMMENT '年份(yyyy)',
            cnt_tem                         INT             COMMENT '有效温度个数',
            tem_mid_yea                     INT             COMMENT '年中位温度',
            tem_avg_yea                     INT             COMMENT '年平均温度',
            tem_dif_yea                     INT             COMMENT '年温度极差',
            tem_var_yea                     INT             COMMENT '年温度方差'
        )
        ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
        "
}
{
    sql_2="
        INSERT OVERWRITE TABLE ${tab_out}
        SELECT      veh                                                                     ,
                    vin                                                                     ,
                    SUBSTR(pdate,1,4)                                   AS  year            ,
                    COUNT(*)                                            AS  cnt_tem         ,
                    CAST(PERCENTILE_APPROX(start_temp,0.5) AS INT)      AS  tem_mid_yea     ,
                    CAST(AVG(start_temp) AS INT)                        AS  tem_avg_yea     ,
                    CAST(PERCENTILE_APPROX(start_temp,0.9) - PERCENTILE_APPROX(start_temp,0.1) AS INT)
                    AS  tem_dif_yea     ,
                    VARIANCE(start_temp)                                AS  tem_var_yea
        FROM        ${tab_in}
        WHERE       start_temp BETWEEN -20 AND 40
        GROUP BY    veh,
                    vin,
                    SUBSTR(pdate,1,4);
        "
}

{
  hive -e "${config};${sql_1};${sql_2}"
}