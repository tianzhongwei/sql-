vehs=('NJL5032XXYBEV')

download[doc://15990 predict3.sh]


for veh in ${vehs[*]};do
    for b in {0..9}
    do
        sh predict3.sh $veh $b 
    done
done
# b=8
# sh predict3.sh $veh $b

# for b in {0..9};do
#     sh predict3.sh $veh $b
# done
# for veh in ${vehs[*]};do
#     for b in {1..10};do
#         sh predict3.sh $veh $b
#     done
# done
# 'BJ7000C5D3-BEV' /    # OK
# 'BYD7003BEV' /        # OK 
# 'BYD7005BEV' /        # OK
# 'BYD7005BEV1' /       # 出错
# 'BYD7005BEV8' /       # OK
# 'HQ7002BEV05' /       # OK
# 'HQ7002BEV08' /       # OK
# 'NJL5032XXYBEV' /     # OK