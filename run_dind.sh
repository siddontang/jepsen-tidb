#!/bin/bash

docker run --privileged -t -i -v $(pwd):/jepsen_tidb --name jepsen siddontang/jepsen_tidb