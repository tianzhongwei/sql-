import sys
import sys
from pyspark.sql import SparkSession
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.linalg import Vectors
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import LinearRegression

def train(tab_name,veh,path):
    # 1 配置
    spark = SparkSession \
        .builder \
        .master("yarn") \
        .appName("tianzw_vol_fading") \
        .config("spark.sql.warehouse.dir","hdfs://neicluster/user/hive/warehouse") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
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
                tem_var_yea     ,
                fading_rate
        FROM    """ + tab_name + """
        WHERE   veh_head = SUBSTR('""" + veh + """',0,1)
        AND     veh = '""" + veh + """'
        AND     rand() < 0.5
            """
    rdd_origin = spark.sql(sql).rdd
    features_rdd = rdd_origin.map(lambda x:
                                  (x.fading_rate,
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
                                       x.tem_var_yea     ,
                                   ])))
    features_list = features_rdd.collect()
    print("数据提取成功")
    spark_df = spark.createDataFrame(features_list,["fading_rate","features"])
    # 3 模型训练
    train_data , test_data = spark_df.randomSplit([1.0,1.0],100)
    #     LR_model = LinearRegression(labelCol = "fading_rate")
    #     model = LR_model.fit(train_data)
    gbt = GBTRegressor(maxIter = 10,
                       maxDepth = 4,
                       labelCol = "fading_rate",
                       seed = 42)
    model = gbt.fit(train_data)
    print("模型训练成功")
    model.write().overwrite().save(path)
    # 4 模型评估
    print(veh + " : 模型写入成功")

if __name__ == "__main__":
    if len(sys.argv) == 2:
        veh = sys.argv[1]
        tab_name = "nei_temp.tianzw_features_veh"
        path = "hdfs://neicluster/user/tianzhongwei_x/vol_fading/model/version2/" + veh + "/"
        train(tab_name = tab_name ,veh = veh, path = path)
    else:
        sys.exit(1)