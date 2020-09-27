{
    tab_in_1="nei_temp.tianzw_vol_fading_rate_filed_20200605"
    tab_in_2="nei_temp.tianzw_battery_charge_feature_1000km_20200605"
    tab_in_3="nei_temp.tianzw_battery_temperture_history_20200605"
    tab_out="nei_temp.tianzw_battery_merged_features_20200605"
}
{
    config="SET mapreduce.job.queuename = product;
            SET mapred.job.name = tianzw_battery_fading;
            set hive.strict.checks.large.query = false;
            set hive.mapred.mode = nonstrict;
            set hive.exec.dynamic.partition.mode=nonstrict;
            "
}

sql_1="
    CREATE TABLE IF NOT EXISTS ${tab_out}
    (
        vin                     STRING      COMMENT '车架号',
        veh                     STRING      COMMENT '车型名',
        sta_time                BIGINT      COMMENT '5080开始时间(s)',
        mils_1000km             INT         COMMENT '1000km数',
        sta_soc                 DOUBLE      COMMENT '开始SOC(1位小数)',
        end_soc                 DOUBLE      COMMENT '结束SOC(1位小数)',
        depth_soc               DOUBLE      COMMENT '充电深度(1位小数)',
        charge_c                DOUBLE      COMMENT '充电倍率(4位小数)',
        hours                   DOUBLE      COMMENT '5080充电小时数(1位小数)',
        temp                    DOUBLE      COMMENT '充电开始环境温度(1位小数)',
        vol                     DOUBLE      COMMENT '5080充入容量(4位小数)',
        fading_rate             DOUBLE      COMMENT '容量衰减百分比(1位小数)',
        days                    INT         COMMENT '经历的天数',
        mils_dif                INT         COMMENT '里程差(km)',
        cnt_cha                 INT         COMMENT '总充电次数',
        vol_cha                 INT         COMMENT '总充电容量',
        vol_avg_cha             DOUBLE      COMMENT '次均充电容量(2位小数)',
        hou_cha                 INT         COMMENT '总充电小时',
        c_avg                   DOUBLE      COMMENT '平均充电倍率(2位小数)',
        sta_soc_avg_cha         INT         COMMENT '平均起始SOC',
        end_soc_avg_cha         INT         COMMENT '平均结束SOC',
        dep_soc_avg_cha         INT         COMMENT '平均SOC深度',
        sta_soc_mid_cha         INT         COMMENT '起始SOC中位数',
        end_soc_mid_cha         INT         COMMENT '结束SOC中位数',
        dep_soc_mid_cha         INT         COMMENT '充电SOC深度中位数',
        cnt_tem                 INT         COMMENT '有效温度个数',
        tem_mid_yea             INT         COMMENT '年中位温度',
        tem_avg_yea             INT         COMMENT '年平均温度',
        tem_dif_yea             INT         COMMENT '年温度极差',
        tem_var_yea             INT         COMMENT '年温度方差'
    )
    PARTITIONED BY (veh_head         STRING      COMMENT '车型名首字母')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
    "


{
    sql_2="
        INSERT OVERWRITE TABLE ${tab_out}
        PARTITION(veh_head)
        SELECT  a.vin                       AS vin,
                a.veh                       AS veh,
                a.sta_time                  AS sta_time,
                a.mils_1000km               AS mils_1000km,
                a.sta_soc                   AS sta_soc,
                a.end_soc                   AS end_soc,
                a.depth_soc                 AS depth_soc,
                a.charge_c                  AS charge_c,
                a.hours                     AS hours,
                a.temp                      AS temp,
                a.vol                       AS vol,
                a.fading_rate               AS fading_rate,
                b.days                      AS days           ,
                b.mils_dif                  AS mils_dif       ,
                b.cnt_cha                   AS cnt_cha        ,
                b.vol_cha                   AS vol_cha        ,
                b.vol_avg_cha               AS vol_avg_cha    ,
                b.hou_cha                   AS hou_cha        ,
                b.c_avg                     AS c_avg          ,
                b.sta_soc_avg_cha           AS sta_soc_avg_cha,
                b.end_soc_avg_cha           AS end_soc_avg_cha,
                b.dep_soc_avg_cha           AS dep_soc_avg_cha,
                b.sta_soc_mid_cha           AS sta_soc_mid_cha,
                b.end_soc_mid_cha           AS end_soc_mid_cha,
                b.dep_soc_mid_cha           AS dep_soc_mid_cha,
                c.cnt_sum                   AS cnt_sum,
                c.tem_mid_yea               AS tem_mid_yea,
                c.tem_avg_yea               AS tem_avg_yea,
                c.tem_dif_yea               AS tem_dif_yea,
                c.tem_var_yea               AS tem_var_yea,
                SUBSTR(a.veh,0,1)           AS veh_head
        FROM    (SELECT             veh,
                                    vin,
                                    sta_time,
                                    mils_1000km,
                                    sta_soc,
                                    end_soc,
                                    depth_soc,
                                    charge_c,
                                    hours,
                                    temp,
                                    vol,
                                    fading_rate
                FROM                ${tab_in_1})          a
                LEFT JOIN(SELECT    veh             ,
                                    vin             ,
                                    mils_1000km     ,
                                    days            ,
                                    mils_min        ,
                                    mils_max        ,
                                    mils_dif        ,
                                    cnt_cha         ,
                                    vol_cha         ,
                                    vol_avg_cha     ,
                                    hou_cha         ,
                                    c_avg           ,
                                    sta_soc_avg_cha ,
                                    end_soc_avg_cha ,
                                    dep_soc_avg_cha ,
                                    sta_soc_mid_cha ,
                                    end_soc_mid_cha ,
                                    dep_soc_mid_cha
                    FROM            ${tab_in_2}
                    WHERE           mils_dif > 500)                                 b
                ON  (a.veh = b.veh AND a.vin = b.vin AND a.mils_1000km = b.mils_1000km)
                LEFT JOIN(SELECT    veh                 AS veh,
                                    vin                 AS vin,
                                    SUM(cnt_tem)        AS cnt_sum,
                                    AVG(tem_mid_yea)    AS tem_mid_yea,
                                    AVG(tem_avg_yea)    AS tem_avg_yea,
                                    AVG(tem_dif_yea)    AS tem_dif_yea,
                                    AVG(tem_var_yea)    AS tem_var_yea
                        FROM        ${tab_in_3}
                        WHERE       cnt_tem > 10
                        GROUP BY    veh,vin)                                        c
                ON  (a.veh = c.veh AND a.vin = c.vin);
        "
}

{
  hive -e "${config};${sql_1};${sql_2}"
}