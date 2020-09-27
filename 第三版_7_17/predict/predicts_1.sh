vehs=('BYD7005BEV1' \
'BYD7005BEV8' \
'BJ7000C5D3-BEV' \
'ZK6805BEVG3' \
'HQ7002BEV05' \
'BYD7005BEV' \
'CSA6454NDPHEV1' \
'BYD7003BEV' \
'HQ7002BEV08' \
'HFC7000W3EV' \
'ZK6805BEVG11' \
'CSA7104SDPHEV3' \
'BYD7150WT5HEV4' \
'BJ7001BPH7-BEV' \
'BYD6460STHEV5' \
'DFM7000G1F3BEV' \
'CSA7002FBEV1' \
'CSA7104SDPHEV1' \
'BJ7001BPH5-BEV' \
'HFC7000WEV' \
'BYD6490STHEV' \
'HQ7002BEV04' \
'BYD7006BEVG' \
'BYD6100LGEV3' \
'HQ7002BEV12' \
'GAH7000BEVH0B')

download[doc://16047 predict.sh]

seg=1
for veh in ${vehs[*]};do
    for i in {0..9}
        do
            name={"tzw_predicts_1_${veh}_${i}_${seg}"}
            #sh predict.sh $veh ${i} ${seg} ${name}
            echo $veh $i $seq $name
    done
    echo ${veh} >> /home/tianzhongwei_x/predicts_1.log
done