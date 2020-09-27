-- 1 将两个表合二为一
-- tab_in_1     nei_temp.tianzw_battery_merged_features_20200605
-- tab_in_2     nei_temp.tianzw_fading_veh
-- tab_out      nei_temp.tianzw_vol_fading_temp

CREATE TABLE IF NOT EXISTS nei_temp.tianzw_vol_fading_temp 
(
    vin         STRING,
    sta_time    BIGINT,
    mils_1000km INT,
    vol_fading  DOUBLE
)
PARTITIONED BY(veh  STRING);


SET mapreduce.job.queuename = product;
SET mapred.job.name = tianzw_battery_fading;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 500;

INSERT OVERWRITE TABLE nei_temp.tianzw_vol_fading_temp 
PARTITION(veh)
SELECT  b.vin               AS vin,
        b.sta_time          AS sta_time,
        a.mils_1000km       AS mils_1000km,
        b.vol_fading        AS vol_fading,
        b.veh               AS veh
FROM    (SELECT  veh_head,
                veh,
                vin,
                sta_time,
                mils_1000km
        FROM    nei_temp.tianzw_battery_merged_features_20200605)               a
        JOIN    (SELECT SUBSTR(veh,0,1)     AS veh_head,
                        veh,
                        vin,
                        sta_time,
                        vol_fading
                FROM    nei_temp.tianzw_fading_veh)             b
        ON      (a.veh_head = b.veh_head AND
                a.veh = b.veh AND 
                a.vin = b.vin AND
                a.sta_time = b.sta_time);

--------------------------- 2 平滑滤波 ----------------------------
-- tab_in_1        nei_temp.tianzw_vol_fading_temp
-- tab_out         nei_temp.tianzw_vol_fading_rolling_with_mils_time
SET hive.strict.checks.large.query to false;
SET hive.mapred.mode = 'nonstrict';

SET mapreduce.job.queuename = product;
SET mapred.job.name = tianzw_battery_fading;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 500;

-- 1 容量衰减滤波
-- 2.1 求和(月数 * 容量衰减)  月数：距离 2017-01-01 的月数
-- 2.2 求和(月数 * 月数)
-- 2.3 最小二乘法：月容量衰减率
-- 3.1 求和(万公里 * 容量衰减)
-- 3.2 求和(万公里 * 万公里)
-- 3.3 最小二乘法求：万公里容量衰减率 = XY / XX

CREATE TABLE IF NOT EXISTS nei_temp.tianzw_vol_fading_rolling_with_mils_time
(
    vin                         STRING,
    sta_time                    BIGINT,
    mils_1000km                 INT,
    vol_fading_rolling          DOUBLE,
    XY_month                    DOUBLE,
    XX_month                    DOUBLE,
    fading_rate_month           DOUBLE,
    XY_mils                     DOUBLE          COMMENT "当前万公里衰减率分母",
    XX_mils                     DOUBLE          COMMENT "当前万公里衰减率分子",
    fading_rate_10000km         DOUBLE          COMMENT "当前万公里衰减率"
)
PARTITIONED BY(veh              STRING);


INSERT OVERWRITE TABLE nei_temp.tianzw_vol_fading_rolling_with_mils_time
PARTITION(veh)
SELECT  vin,
        sta_time,
        mils_1000km,
        ROUND(AVG(vol_fading)                        
            OVER(PARTITION BY vin ORDER BY sta_time ROWS BETWEEN 50 PRECEDING AND 5 FOLLOWING), 6)     
                AS vol_fading_rolling,
        ROUND(SUM(DATEDIFF(FROM_UNIXTIME(sta_time, "yyyy-MM-dd"), "2017-01-01") / 30.4 * vol_fading)
            OVER(PARTITION BY vin ORDER BY sta_time ROWS BETWEEN 50 PRECEDING AND 5 FOLLOWING), 6)
                AS XY_month,
        ROUND(SUM(POW(DATEDIFF(FROM_UNIXTIME(sta_time, "yyyy-MM-dd"), "2017-01-01") / 30.4, 2))
            OVER(PARTITION BY vin ORDER BY sta_time ROWS BETWEEN 50 PRECEDING AND 5 FOLLOWING), 6)
                AS XX_month,
        ROUND(SUM(DATEDIFF(FROM_UNIXTIME(sta_time, "yyyy-MM-dd"), "2017-01-01") / 30.4 * vol_fading)
            OVER(PARTITION BY vin ORDER BY sta_time ROWS BETWEEN 50 PRECEDING AND 5 FOLLOWING) /
                SUM(POW(DATEDIFF(FROM_UNIXTIME(sta_time, "yyyy-MM-dd"), "2017-01-01") / 30.4, 2))
                    OVER(PARTITION BY vin ORDER BY sta_time ROWS BETWEEN 50 PRECEDING AND 5 FOLLOWING), 6)
                        AS fading_rate_month,
        ROUND(SUM((mils_1000km / 10) * (vol_fading)) 
            OVER(PARTITION BY vin ORDER BY sta_time ROWS BETWEEN 50 PRECEDING AND 5 FOLLOWING), 6)
                AS XY_mils,
        ROUND(SUM((mils_1000km / 10) * (mils_1000km / 10))
            OVER(PARTITION BY vin ORDER BY sta_time ROWS BETWEEN 50 PRECEDING AND 5 FOLLOWING), 6)
                AS XX_mils,
        ROUND(SUM((mils_1000km / 10) * (vol_fading)) 
            OVER(PARTITION BY vin ORDER BY sta_time ROWS BETWEEN 50 PRECEDING AND 5 FOLLOWING) /
                SUM((mils_1000km / 10) * (mils_1000km / 10))
                    OVER(PARTITION BY vin ORDER BY sta_time ROWS BETWEEN 50 PRECEDING AND 5 FOLLOWING), 6)
                        AS fading_rate_10000km,
        veh
FROM    nei_temp.tianzw_vol_fading_temp
WHERE   vol_fading BETWEEN -5 AND 40;

--------------------------- 3 半年更新 ----------------------------
-- tab_in      nei_temp.tianzw_vol_fading_rolling_with_mils_time
-- tab_out     nei_temp.tianzw_vol_fading_halfyear
CREATE TABLE IF NOT EXISTS nei_temp.tianzw_vol_fading_halfyear
(
    veh                 STRING,
    vin                 STRING,
    mils_1000km         INT,
    vol_fading          DOUBLE,
    fading_rate_month   DOUBLE,
    fading_rate_10000km DOUBLE
)
PARTITIONED BY(pmonth     STRING);


INSERT OVERWRITE TABLE nei_temp.tianzw_vol_fading_halfyear
PARTITION(pmonth)
SELECT          veh,
                vin,
                CAST(PERCENTILE_APPROX(mils_1000km,0.95) AS INT)                                AS mils_1000km,
                ROUND(CASE  WHEN AVG(vol_fading_rolling) < 0    THEN 0.1                        -- 容量衰减为负值，则默认为 0.1
                            WHEN PERCENTILE_APPROX(mils_1000km,0.95)  < 10 AND AVG(vol_fading_rolling) > 5 THEN 5        -- 1万公里内，容量衰减不超过 5
                            ELSE AVG(vol_fading_rolling) END, 6)                                AS vol_fading,
                ROUND(CASE  WHEN AVG(fading_rate_month) < 0     THEN 0.1                        -- 月容量衰减率为负值，则默认为 0.1 %
                            WHEN PERCENTILE_APPROX(mils_1000km,0.95)  < 10 AND AVG(fading_rate_month) > 0.5 THEN 0.5     -- 1 万公里内，月容量衰减不超过 0.5 %
                            WHEN AVG(fading_rate_month) > 1     THEN 1                          -- 月容量衰减不超过 1 %
                            ELSE AVG(fading_rate_month) END, 6)                                 AS fading_rate_month,
                ROUND(CASE  WHEN AVG(fading_rate_10000km) < 0   THEN 0.1                        -- 万公里容量衰减率为负值，则默认为 0.1
                            WHEN PERCENTILE_APPROX(mils_1000km,0.95)  <= 10 AND AVG(fading_rate_10000km) > 1 THEN 1      -- 1 万公里内，万公里容量衰减率不超过 1%
                            WHEN AVG(fading_rate_10000km) > 5  THEN 5                           -- 万公里容量衰减率不超过 5 %
                            ELSE AVG(fading_rate_10000km) END, 6)                               AS fading_rate_10000km,
                FROM_UNIXTIME(sta_time, "yyyy-MM")                                              AS pmonth
FROM            nei_temp.tianzw_vol_fading_rolling_with_mils_time
WHERE           FROM_UNIXTIME(sta_time, "yyyy-MM") BETWEEN "2020-01" AND "2020-06"              -- 只提取 2020-01 到 2020-06 的数据
GROUP BY        veh,
                vin,
                FROM_UNIXTIME(sta_time, "yyyy-MM");
--------------------------- 4 最终数据 ----------------------------
CREATE TABLE IF NOT EXISTS nei_temp.tianzw_vol_fading_month
(
    vin                 STRING,
    vol_fading_6        DOUBLE,
    vol_fading_7        DOUBLE,
    vol_fading_8        DOUBLE,
    vol_fading_9        DOUBLE,
    vol_fading_10       DOUBLE,
    vol_fading_11       DOUBLE,
    vol_fading_12       DOUBLE,
    veh                 STRING
);

INSERT OVERWRITE TABLE nei_temp.tianzw_vol_fading_month
SELECT  vin,
        ROUND(vol_fading + (6 - substr(pmonth, 6, 2)) * fading_rate_month, 6)          AS vol_fading_6,
        ROUND(vol_fading + (7 - substr(pmonth, 6, 2)) * fading_rate_month, 6)          AS vol_fading_7,
        ROUND(vol_fading + (8 - substr(pmonth, 6, 2)) * fading_rate_month, 6)          AS vol_fading_8,
        ROUND(vol_fading + (9 - substr(pmonth, 6, 2)) * fading_rate_month, 6)          AS vol_fading_9,
        ROUND(vol_fading + (10 - substr(pmonth, 6, 2)) * fading_rate_month, 6)         AS vol_fading_10,
        ROUND(vol_fading + (11 - substr(pmonth, 6, 2)) * fading_rate_month, 6)         AS vol_fading_11,
        ROUND(vol_fading + (12 - substr(pmonth, 6, 2)) * fading_rate_month, 6)         AS vol_fading_12,
        veh
FROM    (SELECT veh                                                                                         AS veh,
                vin                                                                                         AS vin,
                FIRST_VALUE(pmonth)                     OVER(PARTITION BY vin ORDER BY pmonth DESC)         AS pmonth,
                FIRST_VALUE(vol_fading)                 OVER(PARTITION BY vin ORDER BY pmonth DESC)         AS vol_fading,    
                LEAST(FIRST_VALUE(fading_rate_month)    OVER(PARTITION BY vin ORDER BY pmonth DESC), 1)     AS fading_rate_month
        FROM    nei_temp.tianzw_vol_fading_halfyear)    A;
----------------------------------------------------------------------------------------------------