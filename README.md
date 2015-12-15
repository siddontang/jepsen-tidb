# jepsen.tidb

A Clojure library designed to test [TiDB](https://github.com/pingcap/tidb) with [Jepsen](https://github.com/aphyr/jepsen).

## Usage

Jepsen supports two ways to run, LXC or docker. you can see the jepsen document for more help.

If you use docker, you can simply build the docker, and then run with binding the local host source, like:

+ `cd jepsen_tidb`
+ `docker build -t jepsen_tidb ./docker/`
+ `docker run --privileged -t -i -v $(pwd):/jepsen_tidb --name jepsen jepsen_tidb`

After you enter docker jepsen, run:

+ `cd /jepsen_tidb`
+ `lein test`

## License

Copyright © 2015 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
