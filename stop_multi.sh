#!/bin/bash

for i in 1 2 3 4 5
do
    echo "stop jepsen node $i" 
    docker stop jepsen_$i
done

echo "stop jepsen control"
docker stop jepsen_control