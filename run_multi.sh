#!/bin/bash

for i in 1 2 3 4 5
do
    echo "start jepsen node $i"
    docker ps -a | grep -qw jepsen_$i || \
    docker run -d --name jepsen_$i -e ROOT_PASS="root" siddontang/jepsen_node /run.sh

    # if jepsen is not running, start it. 
    docker ps | grep -qw jepsen_$i || docker start jepsen_$i
done 

# docker run -d --name jepsen_n1 -e ROOT_PASS="root" siddontang/jepsen_node /run.sh
# docker run -d --name jepsen_n2 -e ROOT_PASS="root" siddontang/jepsen_node /run.sh
# docker run -d --name jepsen_n3 -e ROOT_PASS="root" siddontang/jepsen_node /run.sh
# docker run -d --name jepsen_n4 -e ROOT_PASS="root" siddontang/jepsen_node /run.sh
# docker run -d --name jepsen_n5 -e ROOT_PASS="root" siddontang/jepsen_node /run.sh

echo "start jepsen control"
docker ps -a | grep -qw jepsen_control || \
docker run -t -i --privileged --name jepsen_control \
    --link jepsen_1:n1 --link jepsen_2:n2 --link jepsen_3:n3 \
    --link jepsen_4:n4 --link jepsen_5:n5 -v $(pwd):/jepsen_tidb siddontang/jepsen_control

docker ps | grep -qw jepsen_control || docker start jepsen_control