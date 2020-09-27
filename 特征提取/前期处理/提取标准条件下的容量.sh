#------- 2 提取标准容量 --------
# 2.1)   提取5000km一下的数据

{
  tab_in="nei_temp.tianzw_battery_first_feature_1000"   # 一级特征
  tab_mid="nei_temp.tianzw_battery_5000km_20200605"     # 0-5000公里的容量
  tab_out="nei_temp.tianzw_veh_standard_vol_20200605"   # 0-5000公里，10-25度，10-90分位，AVG(vol)

  mils_begin=0          # 里程范围 [mils_begin,mils_end]
  mils_end=5000

  temp_begin=10         # 温度范围 [temp_begin,temp_end)
  temp_end=25
}
{
config="
      SET mapreduce.job.queuename = product;
      SET mapred.job.name = tianzw_battery_attenuation;
      set hive.strict.checks.large.query = false;
      set hive.mapred.mode = nonstrict;
      "
}
{
  sql_1="
        CREATE TABLE IF NOT EXISTS ${tab_mid}
        (
          vin         STRING      COMMENT '车架号',
          temp        DOUBLE      COMMENT '开始环境温度(1位小数)',
          hours       DOUBLE      COMMENT '5080充电小时(1位小数)',
          hour_half   INT         COMMENT '5080充电半小时数',
          vol         DOUBLE      COMMENT '5080充入容量(4位小数)',
          veh         STRING      COMMENT '车型名'
        )   COMMENT '5000km以内，5080充入容量与温度、5080充电小时数的关系'
        ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
      "
}

{
  sql_2="
        INSERT OVERWRITE TABLE ${tab_mid}
        SELECT      vin                                                                     AS vin,
                    start_temp                                                              AS temp,
                    ROUND((interval_end_time - interval_start_time) / 3600,1)               AS hours,
                    CAST(ROUND((interval_end_time - interval_start_time) / 1800) AS INT)    AS hour_half,
                    ROUND(interval_volume,4)                                                AS vol,
                    veh                                                                     AS veh
        FROM        ${tab_in}
        WHERE       start_mileage BETWEEN ${mils_begin} AND ${mils_end}
        AND         interval_volume > 0
        AND         start_temp BETWEEN -15 AND 40
        AND         CAST(ROUND((interval_end_time - interval_start_time) / 1800) AS INT) BETWEEN 1 AND 16
        ;
        "
}



# 2.2)   提取5000km以下的数据的特征工程
#  5000km以下，10-25度的标准容量
{
  sql_3="
      CREATE TABLE IF NOT EXISTS ${tab_out}
      (
        veh             STRING      COMMENT '车型名',
        vol_standard    DOUBLE      COMMENT '5000km内，10-25度的标准容量(4位小数)'
      )   COMMENT '5000km以内，15-25度条件下的标准容量'
      ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
      "
}
{
  sql_4="
      INSERT OVERWRITE TABLE ${tab_out}
      SELECT  a.veh                                           AS veh,
              ROUND(AVG(a.vol),4)                             AS vol_standard
      FROM    (SELECT     veh                                 AS veh,
                          temp                                AS temp,
                          vol                                 AS vol
              FROM        ${tab_mid}
              WHERE       temp BETWEEN ${temp_begin} AND ${temp_end})                 a
              INNER JOIN  (SELECT     veh,
                                      PERCENTILE_APPROX(vol,0.2)          AS vol_20,
                                      PERCENTILE_APPROX(vol,0.8)          AS vol_80
                          FROM        ${tab_mid}
                          WHERE       temp BETWEEN ${temp_begin} AND ${temp_end}
                          GROUP BY    veh
              )     b
              ON  (a.veh = b.veh)
      WHERE       a.vol BETWEEN b.vol_20 AND b.vol_80
      GROUP BY    a.veh;
    "
}
{
  #hive -e "${config};${sql_1};${sql_2};${sql_3};${sql_4};"
  hive -e "${config};${sql_2};${sql_3};${sql_4};"
}