vehs=('BJ7000C5D3-BEV' \
'BJ7000U3D-BEV' \
'BJ7001BPH1-BEV' \
'BJ7001BPH7-BEV' \
'BMW7201CMHEV' \
'BYD6100LGEV9' \
'BYD6810LZEV4' \
'BYD7003BEV' \
'BYD7005BEV' \
'BYD7005BEV1' \
'BYD7005BEV8' \
'BYD7150WT5HEV4' \
'CA7001BEV' \
'CK6100LGEV2' \
'CSA7002FBEV4' \
'CSA7154TDPHEV' \
'DFA7000A1F4BEV' \
'DFM7000G1F6BEV' \
'DNC5020XXYBEV01' \
'DXK6450EC4BEV' \
'DYX5040XXYBEV1CAH0' \
'EQ5040XXYACBEV11' \
'EQ5044XXYTBEV2' \
'EQ5070XXYTBEV5' \
'FD5024XXYBEV' \
'GAM6480BEVB0C' \
'GAM7000BEVA0C' \
'GAM7000BEVA0G' \
'GMC7000BEV' \
'GZ6100LGEV5' \
'HFC6483ECEV-W' \
'HFC6502ECEV-W' \
'HFC7000W3EV' \
'HFC7000WEV1' \
'HFC7001AEV' \
'HFC7001EAEV7' \
'HQ7002BEV04' \
'HQ7002BEV05' \
'HQ7002BEV08' \
'HQ7002BEV12' \
'HQG5037XXYEV1' \
'HQG5042XXYEV10' \
'HQG5042XXYEV9' \
'HQG5043XXYEV5' \
'JKC6451AXBEV' \
'JL7001BEV35' \
'JNJ7000EVX25' \
'JX7001ESFBEV' \
'JX7005BEV' \
'JX7009BEV' \
'LZW7001EVABE' \
'LZW7001EVAEP' \
'LZW7002EVBCP' \
'MR7153PHEV01' \
'NEQ7000BEVJ72A' \
'NJL5032XXYBEV' \
'NJL6420BEV5' \
'QCJ7007BEV' \
'QCJ7007BEV3' \
'SC7001AGBEV' \
'SC7003AEBEV' \
'SGM7008LFBEV' \
'SMA7001BEV40' \
'SQR5023XXYBEVK06' \
'SQR7000BEVJ726' \
'SVW7002AEV' \
'THZ7001BEVS001' \
'ZK6105BEVG6' \
'ZK6805BEVG11' \
'ZK6805BEVG3' \
)
download[doc://15673 predict.sh]
cur=0
pre=`tail -n 1 /home/tianzhongwei_x/predict_sup.log`
end=500
for veh in ${vehs[*]};do
  if [ $cur -gt $pre ] && [ $cur -le $end ];then
    echo $veh" "$cur
    sh predict.sh $veh
    echo $cur >> /home/tianzhongwei_x/predict_sup.log
  fi
  ((cur++))
done