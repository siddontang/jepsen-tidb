#!/bin/bash

docker run --privileged -t -i -v $(pwd):/jepsen_tidb --name jepsen_dind siddontang/jepsen_dind