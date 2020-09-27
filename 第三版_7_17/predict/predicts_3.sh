vehs=('CSA7002FBEV3' \
'LZW7001EVA' \
'SC6388BVBEV' \
'SQR7000BEVJ725' \
'SQR7000BEVJ728' \
'BYD7006BEVH' \
'GAM7000BEVA0G' \
'ZK6815BEVG3' \
'SQR7000BEVJ721' \
'ZK6105BEVG6' \
'ZK6805BEVG15' \
'HFC6502ECEV2-W' \
'JX7005BEV' \
'LZW7002EVBBG' \
'HFC7000WEV1' \
'QCJ7007BEV' \
'BJ7000U3D-BEV' \
'SC6458ACBEV' \
'EQ5040XXYACBEV7' \
'DNC5047XXYBEV01' \
'SVW7141BPV' \
'NJL5032XXYBEV1' \
'BJ7001BPH8-BEV' \
'HFC6502ECEV1-W' \
'ZK6105BEVG55' \
'EQ5040XXYACBEV11' \
'SQR5023XXYBEVK06' \
'SQR7000BEVJ729' \
'CSA7102SDPHEV' \
'EQ5045XXYTBEV4' \
'QCJ7007BEV1' \
'HFC6502ECEV-W' \
'BJ7000URD4C-BEV' \
'CGC5044XXYBEV1NBLJEAGY' \
'BYD7150WT5HEV5' \
'DFA7000A1F4BEV' \
'SC7003AABEV' \
'XML6855JEVW0C' \
'JX70016BEV' \
'SC7003AHBEV' \
'CA7002EV' \
'CSA7002FBEV4' \
'LZW7001EVABE' \
'HFC7000EWEV3' \
'GZ6122LGEV' \
'SC7003AEBEV' \
'BYD6810LZEV4' \
'BYD7003BEV4' \
'BJ7000B3D6-BEV' \
'TEG6106BEV11' \
'ZK6105BEVG53' \
'SC7001ADBEV' \
'DNC5045XXYBEV01' \
'BJ7000C5E7-BEV' \
'JX5043XXYTGD25BEV' \
'LZW7000EVA' \
'BJ7001BPH9-BEV' \
'BYD7008BEVA' \
'CC7000CE00BEV' \
'GAC7000BEVH0A' \
'GZ6850HZEV' \
'STJ5024XXYEV' \
'DNC5047XXYBEV05' \
'CRC5030XXYE-LBEV' \
'ZK6815BEVG5' \
'SGM7008LEBEV' \
'CK6100LGEV2' \
'SMA7001BEV23' \
'HKL5041XXYBEV1' \
'BJ7000U3D7-BEV' \
'HQ7002BEV57' \
'BYD6480STHEV' \
'DFA7000L2ABEV4' \
'GZ6100LGEV4' \
'CC7000ZM00ABEV' \
'LF7002PEV' \
'TEG6106BEV20' \
'BYD6100LGEV9' \
'CC6484AD21APHEV' \
'HFC6483ECEV-W')

download[doc://16047 predict.sh]

seg=5
cur=0
pre=`tail -n 1 /home/tianzhongwei_x/predicts_3.log`
end=1
for veh in ${vehs[*]};do
  if [ $cur -gt $pre ] && [ $cur -le $end ];then
    for i in {0..1}
      do
        name="tzw_predicts_3_${veh}_${i}_${cur}"
        sh predict.sh ${veh} ${i} ${seg} ${name}
        #echo $cur ${veh} ${i} ${seg} $name
      done
      echo $cur >> /home/tianzhongwei_x/predicts_3.log
  fi
  ((cur++))
done


# seg=5
# for veh in ${vehs[*]};do
#     for i in {0..1}
#         do
#             sh predict.sh $veh ${i} ${seg}
#     done
#     echo ${veh} >> /home/tianzhongwei_x/predicts_3.log
# done