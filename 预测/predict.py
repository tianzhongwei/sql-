import sys
from pyspark.sql import SparkSession
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.regression import GBTRegressionModel
from pyspark.ml.linalg import Vectors


def predict(features_tab,
            tab_out,
            model_path,
            veh):
    # 1 配置
    spark = SparkSession \
        .builder \
        .master("yarn") \
        .appName("tianzw_vol_fading_predict_second_versions") \
        .config("spark.sql.warehouse.dir","hdfs://neicluster/user/hive/warehouse") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    # 2 准备数据
    sql = """
        SELECT  vin             ,
                sta_time        ,
                mils_1000km     ,
                sta_soc         ,
                charge_c        ,
                hours           ,
                temp            ,
                days            ,
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
                dep_soc_mid_cha ,
                cnt_tem         ,
                tem_mid_yea     ,
                tem_avg_yea     ,
                tem_dif_yea     ,
                tem_var_yea     
        FROM    """ + features_tab + """
        WHERE   veh = '""" + veh + """'
            """
    rdd_origin = spark.sql(sql).rdd
    features_rdd = rdd_origin.map(lambda x:
                                  (x.vin,
                                   x.sta_time,
                                   Vectors.dense([
                                       x.mils_1000km     ,
                                       x.sta_soc         ,
                                       x.charge_c        ,
                                       x.hours           ,
                                       x.temp            ,
                                       x.days            ,
                                       x.mils_dif        ,
                                       x.cnt_cha         ,
                                       x.vol_cha         ,
                                       x.vol_avg_cha     ,
                                       x.hou_cha         ,
                                       x.c_avg           ,
                                       x.sta_soc_avg_cha ,
                                       x.end_soc_avg_cha ,
                                       x.dep_soc_avg_cha ,
                                       x.sta_soc_mid_cha ,
                                       x.end_soc_mid_cha ,
                                       x.dep_soc_mid_cha ,
                                       x.cnt_tem         ,
                                       x.tem_mid_yea     ,
                                       x.tem_avg_yea     ,
                                       x.tem_dif_yea     ,
                                       x.tem_var_yea
                                    ]),))
    features_list = features_rdd.collect()
    print("数据提取成功")
    spark_df = spark.createDataFrame(features_list,["vin","sta_time","features"])
    # 3 模型预测
    # model = GBTRegressor.load(model_path)
    model = GBTRegressionModel.load(model_path)
    print("模型导入成功")
    predictions = model.transform(spark_df)
    print("计算成功")
    new_list = [(x.vin,
                 x.sta_time,
                 x.prediction) for x in predictions.collect()]
    result_df = spark.createDataFrame(new_list,["vin",
                                                "sta_time",
                                                "vol_fading"])
    result_df = result_df.repartition(1)
    result_df.createOrReplaceTempView("table_temp")
    # 数据写入 hive 表
    # createSQL = """
    #             CREATE EXTERNAL TABLE IF NOT EXISTS """ + tab_out + """
    #             (
    #                 vin         STRING      COMMENT '车架号',
    #                 sta_time    BIGINT      COMMENT '充电开始时间(s)',
    #                 vol_fading  DOUBLE      COMMENT '容量衰减百分比预测值(2位小数)'
    #             )
    #             PARTITIONED BY(veh      STRING      COMMENT '车型名')
    #             ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    # """
    insertSql = """
                INSERT OVERWRITE TABLE """ + tab_out + """
                PARTITION(veh = '""" + veh + """')
                SELECT      vin,
                            sta_time,
                            ROUND(vol_fading,2)     AS vol_fading
                FROM        table_temp
    """
    # spark.sql("DROP TABLE IF EXISTS " + tab_out)
    # spark.sql(createSQL)
    spark.sql(insertSql)
    print(tab_out + "写入成功")

if __name__ == "__main__":
    if len(sys.argv) == 2:      # 接收唯一参数 veh
        veh = sys.argv[1]       # 'ZK6805BEVG11'
        features_tab = 'nei_temp.tianzw_battery_merged_features_20200605'
        tab_out = "bitnei_dws.dws_veh_volume_fading_vs_charge"
        model_path = "hdfs://neicluster/user/tianzhongwei_x/vol_fading/model/version2/" + veh + "/"
        predict(features_tab = features_tab,
                tab_out = tab_out,
                model_path = model_path,
                veh = veh)
    else:
        sys.exit(1)