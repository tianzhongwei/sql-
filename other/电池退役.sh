# hive -e"
# drop table nei_temp.retire_battery_forecast;
# CREATE TABLE IF NOT EXISTS nei_temp.retire_battery_forecast
# (privince STRING COMMENT '省',
# city STRING COMMENT '市',
# veh_cnt INT COMMENT '车辆数',
# pack_cnt INT COMMENT '电池包数',
# module_cnt INT COMMENT '电池模块数',
# cell_cnt INT COMMENT '单体数',
# month STRING COMMENT 'yyyy-MM'
# )ROW FORMAT SERDE'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
# WITH SERDEPROPERTIES ('field.delim'='\t','serialization.format'=',','serialization.null.format'='')
# LOCATION 'hdfs://neicluster/user/hive/warehouse/nei_temp.db/retire_battery_forecast';
# "

#sync[16065->360]
QUEUENAME="product"
# tidb
USER_NAME="battery_user"
PASSWORD="k3PqEEBuqozryptk"
IP_PATH="10.10.21.8"
PORT="4004"
DATABASE_NAME="battery_platform"
TABLE_NAME="retire_battery_forecast"
##################################
MYSQL_CMD="mysql -h$IP_PATH -P$PORT -u$USER_NAME -p$PASSWORD"
MYSQL_DDL="
DROP TABLE IF EXISTS battery_platform.retire_battery_forecast;
create table battery_platform.retire_battery_forecast(
 province varchar(64) DEFAULT NULL COMMENT '省份',
 city varchar(64) DEFAULT NULL COMMENT '城市',
 veh_cnt int(11) DEFAULT NULL COMMENT '退役车辆数',
 pack_cnt int(11) DEFAULT NULL COMMENT '退役电池包数',
 module_cnt int(11) DEFAULT NULL COMMENT '退役模块数',
 cell_cnt int(11) DEFAULT NULL COMMENT '退役单体数',
 month varchar(64) DEFAULT NULL COMMENT '月份'
)ENGINE = INNODB DEFAULT CHARSET = utf8 COLLATE = utf8_bin COMMENT = '电池退役';
create INDEX u1_index on battery_platform.retire_battery_forecast(province, city);"
$MYSQL_CMD -e "$MYSQL_DDL"
####################################

source_dir="hdfs://neicluster/user/hive/warehouse/nei_temp.db/retire_battery_forecast"
JDBC_URL="jdbc:mysql://$IP_PATH:$PORT/${DATABASE_NAME//\`/}?characterEncoding=UTF-8"
sqoop-export -D sqoop.export.records.per.statement=300 \
 -D mapred.job.queue.name=$QUEUENAME \
 --connect $JDBC_URL \
 --username $USER_NAME \
 --password $PASSWORD \
 --table $TABLE_NAME \
 --export-dir $source_dir \
 --input-fields-terminated-by '\t' \
 --input-lines-terminated-by '\n' \
 --mapreduce-job-name "admin_同步tidb:$DATABASE_NAME.$TABLE_NAME" \
 -m 120 \
 --batch