vehs=('SCH5022XXY-BEV8' \
'SQR7000BEVJ601' \
'SQR5020XXYBEVK061' \
'BYD6121LGEV3' \
'BYD7150WTHEV3' \
'DN7007HBEVS3' \
'CC7001CE02ABEV' \
'SC7001CABBEV' \
'HFC7000BEV5' \
'YTK5040XXYEV2' \
'SC7003ACBEV' \
'BJ7001BPH2-BEV' \
'BYD6480STHEV5' \
'SQR7000BEVJ726' \
'SQR7000BEVJ727' \
'BJ5030XXYVRRC-BEV' \
'GTM7180LCHEVM' \
'HMA7002S68BEV' \
'CC7000CE04BEV' \
'SGM7158DACHEV' \
'JX7006BEV' \
'HFC7001EAEV2' \
'CC7000ZM00CBEV' \
'YQZ7201PHEV' \
'LZW7001EVAEP' \
'BYD6480STHEV3' \
'CRC5034XXYD-LBEV' \
'JKC6451AXBEV' \
'HFC7001A1EV' \
'BYD7008BEV3' \
'YGM6371BEV' \
'TV7186HEV6' \
'HMA7003S10BEV' \
'BYD6440SBEV2' \
'SC7001CAABEV' \
'MR6471PHEV04' \
'SC7001AGBEV' \
'CRC5034XXYC-LBEV' \
'SMA7001BEV25' \
'CSA7154SEPHEV2' \
'BYD6122LGEV1' \
'NEQ7000BEVJ72A' \
'DN7004MBEV' \
'JNJ7000EVX16' \
'SCH5022XXY-BEVC' \
'XML5036XXYEVL03' \
'SVW6471APV' \
'JHC7002BEV33' \
'BJ7000C5E4-BEV' \
'DXK6450EC5BEV' \
'BYD6100LGEV10' \
'BYD7152WT6HEVB1' \
'BJ7000C5E7G-BEV' \
'SMA7000BEV05' \
'FJ5020XXYBEVA6' \
'HQ7002BEV35' \
'LCK6809EVG3A6' \
'SC6458AHBEV' \
'LBA6431BABEV' \
'CC7000ZM01BEV' \
'DFA7000A1F7BEV' \
'FJ5020XXYBEVA12' \
'BYD3310EH9BEV2' \
'HQ7002BEV37' \
'JNJ7000EVX13' \
'JX70034BEV' \
'HFC7001EAEV5' \
'HQG5042XXYEV9' \
'LZW7001EVABP' \
'CRC5021XXYA-LBEV' \
'DFA7000L2ABEV' \
'BJ7001BPHA-BEV' \
'YDE7000BEV1H' \
'SQR5025XXYBEVK06' \
'NJL6420BEV5' \
'BYD6461ST6HEV1' \
'SC7004CABEV' \
'HMA7000S68BEV' \
'DFA5040XXYKBEV2' \
'CGC5044XXYBEV1NBLJEAGK' \
'HFC6483ECEV2-W' \
'BYD6460SBEV7' \
'LCK6809EVG3A7' \
'QCJ7007BEV2' \
'MR7152PHEV01' \
'SC6458AGBEV' \
'EQ5022XXYTBEV1')


download[doc://16047 predict.sh]

seg=5
for veh in ${vehs[*]};do
    for i in {0..1}
        do
            sh predict.sh $veh ${i} ${seg}
    done
    echo ${veh} >> /home/tianzhongwei_x/predicts_4.log
done