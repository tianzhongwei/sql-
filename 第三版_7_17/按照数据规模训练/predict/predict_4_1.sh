vehs=('ZK6105BEVG57' \
'JL7001BEV45' \
'DXK6450EC1BEV' \
'NJL5038XXYBEV4' \
'JL7001BEV35' \
'BJ7000U3D2-BEV' \
'BYD6490ST6HEV1' \
'JNJ7000EVX9' \
'SQR7001BEVJ72' \
'ZK6115BEVG16' \
'NJL5032XXYBEV' \
'CSA6461FBEV1' \
'JL6453PHEV08' \
'JX7009BEV' \
'BYD7003BEV3' \
'EQ5045XXYTBEV3' \
'FJ5020XXYBEVA5' \
'SQR5024XXYBEVK06' \
'CA5020XXYBEV31' \
'HMA7003S201BEV' \
'GAC6450CHEVA5E' \
'SCH5022XXY-BEV1' \
'HFC7000AEV' \
'NJ5047XXYCEV3' \
'LCK6816EVGA' \
'JX70015BEV' \
'NEQ7000BEVJ72B' \
'BMW7201BMHEV' \
'SC6388CVBEV' \
'HFC7000BEV7' \
'CA7001BEV' \
'LCK5049XXYEVH3' \
'JNJ7000EVA9' \
'DFL7000NA62BEV' \
'BYD7008BEVA4' \
'DN7007MBEV' \
'BJ7000C5ED-BEV' \
'YGM7000BEVA4' \
'SZS6460A20BEV' \
'BJ7000C5E9-BEV' \
'SH5047XXYZFEVNZ' \
'HQG5042XXYEV10' \
'JNJ7000EVX14' \
'HMA7001DEBEV' \
'BYD7003BEV1' \
'SDH7000BEVYL' \
'GAC6450CHEVA5D' \
'JHC7002BEV60' \
'HFC7001AEV3' \
'HFC7001E1AEV' \
'CSA6456BEV2' \
'EQ5023XXYACBEV1' \
'ZK6815BEVG13' \
'CSA6454NEPHEV2' \
'HFC7001AEV' \
'EQ5045XXYTBEV12' \
'DFL7000NAH2BEV' \
'LZW7002EVBHH' \
'CGC5044XXYBEV1AAAJEAHZ' \
'BYD6490ST6HEV' \
'ZK6815BEVG9A' \
'EQ5044XXYTBEV2' \
'DXK6450EC6BEV' \
'BYD6460SBEV' \
'ZK6105BEVG59' \
'CA5040XXYBEV31' \
'SQR7000BEVJ602' \
'JNJ7000EVK9' \
'JX70023BEV' \
'BMW6462ACHEV(BMWX1)' \
'ZK6850BEVG57A' \
'DN7008MBEVS' \
'LZW7002EVBEP' \
'BYD6470MTHEV1' \
'BYD7008BEV1' \
'MR7153PHEV08' \
'SC1027DACABEV' \
'YQZ7003BEV' \
'JNJ7000EVK1' \
'SGM7200KACHEV' \
'BYD7150WT5HEV6' \
'SH5041XXYA7BEV-6' \
'SQR5026XXYBEVK06' \
'EQ5020XXYLBEV1' \
'BJ7001BPHD-BEV' \
'BJ7000C5E2G-BEV' \
'LZ7000SLAEV' \
'QCJ7007BEV3' \
'SC7001AKBEV' \
'SDH6440BEVGL' \
'GMC6450ACHEV' \
'HQ7002BEV51' \
'CSA7144CDPHEV1' \
'LF7004GEV' \
'SC7004DBBEV' \
'HMA7001S68BEV' \
'JX70036BEV' \
'HX5021XXYVEV' \
'CC7001CE03ABEV' \
'HFC7000EWEV4')


download[doc://16047 predict.sh]

seg=10
for veh in ${vehs[*]};do
    for i in {0}
        do
            sh predict.sh $veh ${i} ${seg}
    done
    echo ${veh} >> /home/tianzhongwei_x/predict_4.log
done