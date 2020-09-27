#!/usr/bin/env bash

haoop fs  -rm -r /user/tianzhongwei_x/data
hadoop fs -mkdir -p /user/tianzhongwei_x/data

hive -e "
      SET mapreduce.job.queuename = dev;
      SET mapred.job.name = tianzw_data_extraction;
      SET hive.cli.print.header = true;
      SET hive.resultset.use.unique.column.names = false;
      SET hive.mapred.mode = nonstrict;
      SET mapreduce.map.memory.mb = 4048;
      SET mapreduce.reduce.memory.mb = 8096;
      INSERT OVERWRITE DIRECTORY 'hdfs://neicluster/user/tianzhongwei_x/data/'
      SELECT    md5(vin)                ,
                sta_time       ,
                mils_1000km    ,
                sta_soc        ,
                end_soc        ,
                depth_soc      ,
                charge_c       ,
                hours          ,
                temp           ,
                vol            ,
                fading_rate    ,
                days           ,
                mils_dif       ,
                cnt_cha        ,
                vol_cha        ,
                vol_avg_cha    ,
                hou_cha        ,
                c_avg          ,
                sta_soc_avg_cha,
                end_soc_avg_cha,
                dep_soc_avg_cha,
                sta_soc_mid_cha,
                end_soc_mid_cha,
                dep_soc_mid_cha,
                cnt_tem        ,
                tem_mid_yea    ,
                tem_avg_yea    ,
                tem_dif_yea    ,
                md5(veh)
        FROM    nei_temp.tianzw_battery_merged_features_20200605
        WHERE   from_unixtime(sta_time, 'yyyy-MM-dd') BETWEEN '2020-01-01' AND '2020-07-01';
        "


rm -rf /home/tianzhongwei_x/data/
mkdir -p /home/tianzhongwei_x/data/

hadoop fs -cat /user/tianzhongwei_x/data/* > /home/tianzhongwei_x/data/test.data