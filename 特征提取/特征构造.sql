----------------------   1 当前状态    ----------------------
----------------------   2 近期历史    ----------------------
-- (1)  充电特征
DROP TABLE IF EXISTS nei_temp.tianzw_battery_charge_feature_1000km;
CREATE TABLE nei_temp.tianzw_battery_charge_feature_1000km
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
  dep_soc_mid_cha     INT         COMMENT '充电SOC深度中位数'
)
PARTITIONED BY(veh      STRING      COMMENT '车型名')
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';


SET mapreduce.job.queuename = product;
SET mapred.job.name = tianzw_battery_attenuation;
SET hive.mapred.mode = nonstrict;
--SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 100;

INSERT OVERWRITE TABLE nei_temp.tianzw_battery_charge_feature_1000km
PARTITION(veh)
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
                  veh
  FROM            nei_temp.tianzw_battery_first_feature
  WHERE           ((et_time_e - st_time_e) / 60) BETWEEN 5 AND 1440       -- 充电时长 [5,1440] min
  AND             (end_soc - start_soc) BETWEEN 1 AND 100                 -- 充电点深度 [1,100] soc
  GROUP BY        veh,
                  vin,
                  CAST(start_mileage / 1000 AS INT);


-- (2)  行驶

----------------------   3 环境特征    ----------------------
-- [-20,40] 范围内，[10分位,90分位]
DROP TABLE IF EXISTS nei_temp.tianzw_battery_temperture_history;
CREATE TABLE IF NOT EXISTS nei_temp.tianzw_battery_temperture_history
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
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

SET mapreduce.job.queuename = product;
SET mapred.job.name = tianzw_battery_attenuation;
SET hive.mapred.mode = nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 100;

INSERT OVERWRITE TABLE nei_temp.tianzw_battery_temperture_history
  SELECT      veh                                                                     ,
              vin                                                                     ,
              SUBSTR(pdate,1,4)                                   AS  year            ,
              COUNT(*)                                            AS  cnt_tem         ,
              CAST(PERCENTILE_APPROX(start_temp,0.5) AS INT)      AS  tem_mid_yea     ,
              CAST(AVG(start_temp) AS INT)                        AS  tem_avg_yea     ,
              CAST(PERCENTILE_APPROX(start_temp,0.9) - PERCENTILE_APPROX(start_temp,0.1) AS INT)
              AS  tem_dif_yea     ,
              VARIANCE(start_temp)                                AS  tem_var_yea
  FROM        nei_temp.tianzw_battery_first_feature
  WHERE       start_temp BETWEEN -20 AND 40
  GROUP BY    veh,
  vin,
  SUBSTR(pdate,1,4);


----------------------   4 合并特征    ----------------------

DROP TABLE IF EXISTS nei_temp.tianzw_battery_merged_features;
CREATE TABLE IF NOT EXISTS nei_temp.tianzw_battery_merged_features
(
  -- 1 本次充电特征
  vin                     STRING      COMMENT '车架号',
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
  -- 2 近期(同1000km内)历史
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
  -- 3 年环境温度
  cnt_tem                 INT         COMMENT '有效温度个数',
  tem_mid_yea             INT         COMMENT '年中位温度',
  tem_avg_yea             INT         COMMENT '年平均温度',
  tem_dif_yea             INT         COMMENT '年温度极差',
  tem_var_yea             INT         COMMENT '年温度方差'
)
PARTITIONED BY (veh         STRING      COMMENT '车型名')
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';


SET mapreduce.job.queuename = product;
SET mapred.job.name = tianzw_battery_attenuation;
SET hive.mapred.mode = nonstrict;
--SET hive.exec.dynamic.partition.mode = nonstrict
SET hive.exec.max.dynamic.partitions.pernode = 100;


INSERT OVERWRITE TABLE nei_temp.tianzw_battery_merged_features
PARTITION(veh)
  SELECT      -- 1 本次充电特征
  a.vin                       AS vin,
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
  -- 2 近期(同1000km内)历史
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
  -- 3 年环境温度
  c.cnt_sum                   AS cnt_sum,
  c.tem_mid_yea               AS tem_mid_yea,
  c.tem_avg_yea               AS tem_avg_yea,
  c.tem_dif_yea               AS tem_dif_yea,
  c.tem_var_yea               AS tem_var_yea,
  -- 附 : 分区
  a.veh                       AS veh
  FROM        (SELECT             veh,
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
               FROM                nei_temp.tianzw_vol_fading_rate_filed)          a
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
              FROM        nei_temp.tianzw_battery_charge_feature_1000km
              WHERE       mils_dif > 500)                                 b
      ON  (a.veh = b.veh AND a.vin = b.vin AND a.mils_1000km = b.mils_1000km)
    LEFT JOIN(SELECT    veh                 AS veh,
                        vin                 AS vin,
                        SUM(cnt_tem)        AS cnt_sum,
                        AVG(tem_mid_yea)    AS tem_mid_yea,
                        AVG(tem_avg_yea)    AS tem_avg_yea,
                        AVG(tem_dif_yea)    AS tem_dif_yea,
                        AVG(tem_var_yea)    AS tem_var_yea
              FROM        nei_temp.tianzw_battery_temperture_history
              WHERE       cnt_tem > 10        -- 年有效温度个数 > 10
              GROUP BY    veh,vin)                                        c
      ON  (a.veh = c.veh AND a.vin = c.vin);