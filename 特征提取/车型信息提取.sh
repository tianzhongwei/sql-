#!/usr/bin/env bash
# 1 接收参数
{
    tab_in="bitnei_dwd.dwd_fact_vehicle"
    tab_out="nei_temp.tianzw_battery_veh_20200605"
}
# 2 配置
{
  config="SET mapreduce.job.queuename = product;
          SET mapred.job.name = tianzw_vol_fading;"
}
{
sql_1="
    CREATE EXTERNAL TABLE IF NOT EXISTS ${tab_out}
    (
        vin                 STRING      COMMENT '车架号',          
        entry_date          STRING      COMMENT '单车注册时间',    
        veh_category        STRING      COMMENT '车辆用途',        
        veh                 STRING      COMMENT '车型',            
        battery_type        STRING      COMMENT '储能装置种类',    
        vin_nums            STRING      COMMENT '车型车辆数',      
        entry_veh           STRING      COMMENT '车型最早注册时间',
        power_type          STRING      COMMENT '能量类型',        
        rating_volume       STRING      COMMENT '标称容量(Ah)',    
        cell_enterprise     STRING      COMMENT '电池单体生产企业名称',   
        drive_range_level   STRING      COMMENT '续航级别'                
    )COMMENT '车型信息表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
    "
sql_2="
    INSERT OVERWRITE TABLE ${tab_out}
    SELECT  vin                                                   AS vin,          
            entry_date                                            AS entry_date,   
            veh_category                                          AS veh_category, 
            veh_model_name                                        AS veh,          
            battery_type                                          AS battery_type, 
            COUNT(vin) OVER(PARTITION BY veh_model_name)          AS vin_nums,     
            MIN(entry_date) OVER(PARTITION BY veh_model_name)     AS entry_veh,    
            power_type                                            AS power_type,   
            rating_volume                                         AS rating_volume,
            cell_enterprise                                       AS cell_enterprise,
            drive_range_level                                     AS drive_range_level
    FROM    ${tab_in}
    "
}
{
    hive -e "${config};${sql_1};${sql_2}"
}