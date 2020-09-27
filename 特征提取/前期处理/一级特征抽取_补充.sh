############ 1 一级特征提取 ############
# 1.1 建一级特征表
{
  tab_in_1="nei_dwd.fact_realinfo_split"            # 切分表
  tab_in_2="nei_temp.tianzw_battery_veh_20200605"   # 车型信息表

  date_begin="2020-01-01"                           # 数据起始时间 
  date_end="2020-07-01"                             # 数据截至时间(不包含)

  entry_veh_begin="2017-01-01"                      # 车型注册时间上限
  entry_veh_end="2020-01-01"                        # 车型注册时间下限(不包含)
  
  vin_nums_begin=1000                               # 车型的车辆数范围 [vin_nums_begin,vin_nums_end)
  vin_nums_end=1000000
  
  tab_out="nei_temp.tianzw_battery_first_feature_${vin_nums_begin}"   # 初级特征表，按 pmonth 分区
}

{
  config="SET mapreduce.job.queuename = product;
          SET mapred.job.name = tianzw_battery_fading_${date_begin};
          SET hive.exec.dynamic.partition.mode = nonstrict;
          SET hive.exec.max.dynamic.partitions.pernode = 400;"
}
{
  sql_1="
      CREATE TABLE IF NOT EXISTS ${tab_out}
      (
        vin                     STRING      COMMENT '车架号',
        veh                     STRING      COMMENT '车型名',
        st_time_e               BIGINT      COMMENT '充电开始时间(s)',
        et_time_e               BIGINT      COMMENT '充电结束时间(s)',
        start_soc               DOUBLE      COMMENT '充电开始SOC(2位小数)',
        end_soc                 DOUBLE      COMMENT '充电开始SOC(2位小数)',
        volume                  DOUBLE      COMMENT '充入容量(4位小数)',
        start_mileage           INT         COMMENT '充电开始里程(km)',
        charge_c                DOUBLE      COMMENT '充电倍率(4位小数)',
        start_temp              DOUBLE      COMMENT '开始环境温度(1位小数)',
        interval_start_time     BIGINT      COMMENT '50开始充电时间(s)',
        interval_end_time       BIGINT      COMMENT '80结束充电时间(s)',
        interval_start_mile     INT         COMMENT '50开始充电里程(km)',
        interval_volume         DOUBLE      COMMENT '5080充入电容量(4位小数)',
        pdate                   STRING      COMMENT '日期(yyyy-MM-dd)'
      )   COMMENT '电池衰减/一级特征'
      PARTITIONED BY (pmonth     STRING COMMENT 'yyyy-MM')
      ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
      "
}
{
  sql_2="
        INSERT OVERWRITE TABLE ${tab_out}
        PARTITION(pmonth)
        SELECT  a.vin                                               AS  vin                     ,
                a.veh                                               AS  veh                     ,
                CAST(b.st_time_e / 1000 AS BIGINT)                  AS  st_time_e               ,
                CAST(b.et_time_e / 1000 AS BIGINT)                  AS  et_time_e               ,
                ROUND(b.start_soc,2)                                AS  start_soc               ,
                ROUND(b.end_soc,2)                                  AS  end_soc                 ,
                ROUND(b.volume,4)                                   AS  volume                  ,
                CAST(b.start_mileage / 10 AS INT)                   AS  start_mileage           ,
                ROUND(b.charge_c,4)                                 AS  charge_c                ,
                ROUND(b.start_temp,1)                               AS  start_temp              ,
                CAST(b.interval_start_time / 1000 AS BIGINT)        AS  interval_start_time     ,
                CAST(b.interval_end_time / 1000 AS BIGINT)          AS  interval_end_time       ,
                CAST(b.interval_start_mile / 10 AS INT)             AS  interval_start_mile     ,
                ROUND(b.interval_volume,4)                          AS  interval_volume         ,
                b.pdate                                             AS  pdate                   ,
                b.pmonth                                            AS  pmonth
        FROM    (SELECT     vin,
                            veh
                FROM        ${tab_in_2}
                WHERE       ${vin_nums_begin} <= vin_nums AND vin_nums < ${vin_nums_end}
                AND         '${entry_veh_begin}' <= entry_veh AND entry_veh < '${entry_veh_end}')  a
                INNER JOIN (SELECT  vin                     ,
                                    st_time_e               ,
                                    et_time_e               ,
                                    start_soc               ,
                                    end_soc                 ,
                                    volume                  ,
                                    start_mileage           ,
                                    charge_c                ,
                                    start_temp              ,
                                    interval_start_time     ,
                                    interval_end_time       ,
                                    interval_start_mile     ,
                                    interval_volume         ,
                                    pdate                   ,
                                    SUBSTR(pdate,0,7)       AS pmonth
                            FROM    ${tab_in_1}
                            WHERE   '${date_begin}' <= pdate AND pdate < '${date_end}'
                            AND     category = 30
                            AND     end_soc - start_soc > 5)    b
                ON  (a.vin = b.vin);
        "
}
{
  hive -e "${config};${sql_2}"
}