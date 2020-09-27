# ------- 构造当前充电状态特征 --------
{
    tab_in_1="nei_temp.tianzw_battery_first_feature_1000"
    tab_in_2="nei_temp.tianzw_veh_standard_vol_20200605"
    tab_out="nei_temp.tianzw_vol_fading_rate_filed_20200605"

    max_mils=500000
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
            sta_time            BIGINT      COMMENT '5080开始时间(s)',
            mils_1000km         INT         COMMENT '1000km数',
            sta_soc             DOUBLE      COMMENT '开始SOC(1位小数)',
            end_soc             DOUBLE      COMMENT '结束SOC(1位小数)',
            depth_soc           DOUBLE      COMMENT '充电深度(1位小数)',
            charge_c            DOUBLE      COMMENT '充电倍率(4位小数)',
            hours               DOUBLE      COMMENT '5080充电小时数(1位小数)',
            temp                DOUBLE      COMMENT '充电开始环境温度(1位小数)',
            vol                 DOUBLE      COMMENT '5080充入容量(4位小数)',
            fading_rate         DOUBLE      COMMENT '容量衰减百分比(1位小数)',
            veh                 STRING      COMMENT '车型名'
        )   COMMENT '[0,50]万公里，温度[-15,40]，衰减率[-50,50]，充电时长[0.25,8]，5080充电段'
        PARTITIONED BY(veh_head  STRING  COMMENT '车型名首字母')
        ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
        "
    sql_2="
        INSERT OVERWRITE TABLE ${tab_out}
        PARTITION(veh_head)
        SELECT	a.vin                                       AS vin,
                a.sta_time                                  AS sta_time,
                a.mils_1000km                               AS mils_1000km,
                a.sta_soc                                   AS sta_soc,
                a.end_soc                                   AS end_soc,
                ROUND(a.end_soc - a.sta_soc,1)              AS depth_soc,
                a.charge_c                                  AS charge_c,
                a.hours                                     AS hours,
                a.temp                                      AS temp,
                a.vol                                       AS vol,
                ROUND((1 - a.vol / b.vol_standard) * 100,1) AS fading_rate,
                a.veh                                       AS veh,
                substr(a.veh,0,1)                           AS veh_head
        FROM	(SELECT     vin                                                             AS vin,
                            interval_start_time                                             AS sta_time,
                            CAST(interval_start_mile / 1000 AS INT)                         AS mils_1000km,
                            start_soc                                                       AS sta_soc,
                            end_soc                                                         AS end_soc,
                            charge_c                                                        AS charge_c,
                            ROUND((interval_end_time - interval_start_time) / 3600,1)       AS hours,
                            start_temp                                                      AS temp,
                            interval_volume                                                 AS vol,
                            veh                                                             AS veh
                FROM        ${tab_in_1}
                WHERE       interval_start_mile BETWEEN 0 AND ${max_mils}
                AND         interval_volume > 0
                AND         start_temp BETWEEN -15 AND 40
                AND         (interval_end_time - interval_start_time) / 3600 BETWEEN 0.25 AND 8)                a
                INNER JOIN  (SELECT     veh,
                                        vol_standard
                            FROM        ${tab_in_2})                                                            b
                ON          (a.veh = b.veh)
        WHERE (a.vol BETWEEN b.vol_standard * 0.5 AND b.vol_standard * 1.5)
        AND	(
                    (a.vol / b.vol_standard BETWEEN 0.5 AND 1.1)
                    OR (a.vol / b.vol_standard BETWEEN 1.1 AND 1.3 AND RAND() < 0.5)
                    OR (a.vol / b.vol_standard BETWEEN 1.3 AND 1.5 AND a.mils_1000km BETWEEN 20 AND 40 AND RAND() < 0.9)
                    OR (a.vol / b.vol_standard BETWEEN 1.3 AND 1.5 AND a.mils_1000km BETWEEN 40 AND 60 AND RAND() < 0.8)
                    OR (a.vol / b.vol_standard BETWEEN 1.3 AND 1.5 AND a.mils_1000km BETWEEN 60 AND 80 AND RAND() < 0.7)
                    OR (a.vol / b.vol_standard BETWEEN 1.3 AND 1.5 AND a.mils_1000km BETWEEN 80 AND 100 AND RAND() < 0.6)
                    OR (a.vol / b.vol_standard BETWEEN 1.3 AND 1.5 AND a.mils_1000km BETWEEN 100 AND 120 AND RAND() < 0.5)
                    OR (a.vol / b.vol_standard BETWEEN 1.3 AND 1.5 AND a.mils_1000km BETWEEN 120 AND 140 AND RAND() < 0.4)
                    OR (a.vol / b.vol_standard BETWEEN 1.3 AND 1.5 AND a.mils_1000km BETWEEN 140 AND 180 AND RAND() < 0.3)
                    OR (a.vol / b.vol_standard BETWEEN 1.3 AND 1.5 AND a.mils_1000km BETWEEN 180 AND 200 AND RAND() < 0.2)
            );
        "
}

{
  hive -e "${config};${sql_1};${sql_2}"
}