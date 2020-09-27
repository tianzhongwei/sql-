{
    tab_in="nei_temp.tianzw_battery_first_feature_1000"
    tab_out="nei_temp.tianzw_battery_charge_feature_1000km_20200605"
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
            vin                 STRING      COMMENT '车架号',
            mils_1000km         INT         COMMENT '1000km数',
            days                INT         COMMENT '经历的天数',
            mils_min            INT         COMMENT '最小里程(km)',
            mils_max            INT         COMMENT '最大里程(km)',
            mils_dif            INT         COMMENT '里程差(km)',
            cnt_cha             INT         COMMENT '总充电次数',
            vol_cha             INT         COMMENT '总充电容量',
            vol_avg_cha         DOUBLE      COMMENT '次均充电容量(2位小数)',
            hou_cha             INT         COMMENT '总充电小时',
            c_avg               DOUBLE      COMMENT '平均充电倍率(2位小数)',
            sta_soc_avg_cha     INT         COMMENT '平均起始SOC',
            end_soc_avg_cha     INT         COMMENT '平均结束SOC',
            dep_soc_avg_cha     INT         COMMENT '平均SOC深度',
            sta_soc_mid_cha     INT         COMMENT '起始SOC中位数',
            end_soc_mid_cha     INT         COMMENT '结束SOC中位数',
            dep_soc_mid_cha     INT         COMMENT '充电SOC深度中位数',
            veh                 STRING      COMMENT '车型名'
        )
        PARTITIONED BY(veh_head      STRING      COMMENT '车型名首字母')
        ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
        "
}

{
    sql_2="
        INSERT OVERWRITE TABLE ${tab_out}
        PARTITION(veh_head)
        SELECT          vin                                                         AS      vin,
                        CAST(start_mileage / 1000 AS INT)                           AS      mils_1000km,
                        CAST((MAX(st_time_e) - MIN(st_time_e)) / 3600 / 24 AS INT)  AS      days,
                        MIN(start_mileage)                                          AS      mils_min,
                        MAX(start_mileage)                                          AS      mils_max,
                        MAX(start_mileage) - MIN(start_mileage)                     AS      mils_dif,
                        COUNT(*)                                                    AS      cnt_cha,
                        SUM(volume)                                                 AS      vol_cha,
                        ROUND(SUM(volume) / COUNT(*),2)                             AS      vol_avg_cha,
                        CAST(SUM(et_time_e - st_time_e) / 3600 AS INT)              AS      hou_cha,
                        ROUND(AVG(charge_c),2)                                      AS      c_avg,
                        CAST(AVG(start_soc) AS INT)                                 AS      sta_soc_avg_cha,
                        CAST(AVG(end_soc) AS INT)                                   AS      end_soc_avg_cha,
                        CAST(AVG(end_soc - start_soc) AS INT)                       AS      dep_soc_avg_cha,
                        CAST(PERCENTILE_APPROX(start_soc,0.5) AS INT)               AS      sta_soc_mid_cha,
                        CAST(PERCENTILE_APPROX(end_soc,0.5) AS INT)                 AS      end_soc_mid_cha,
                        CAST(PERCENTILE_APPROX(end_soc - start_soc,0.5) AS INT)     AS      dep_soc_mid_cha,
                        veh                                                         AS      veh,
                        SUBSTR(veh,0,1)                                             AS      veh_head
        FROM            ${tab_in}
        WHERE           ((et_time_e - st_time_e) / 60) BETWEEN 5 AND 1440 
        AND             (end_soc - start_soc) BETWEEN 1 AND 100 
        GROUP BY        veh,
                        vin,
                        CAST(start_mileage / 1000 AS INT);
        "
}

{
  hive -e "${config};${sql_1};${sql_2}"
}