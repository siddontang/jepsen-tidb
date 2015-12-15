(ns jepsen.tidb
  "Tests for TiDB"
  (:require [clojure.tools.logging :refer :all]
            [clojure.core.reducers :as r]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [knossos.op :as op]
            [jepsen [client :as client]
             [core :as jepsen]
             [db :as db]
             [tests :as tests]
             [control :as c :refer [|]]
             [checker :as checker]
             [nemesis :as nemesis]
             [generator :as gen]
             [util :refer [timeout meh]]]
            [jepsen.control.util :as cu]
            [jepsen.control.net :as cn]
            [jepsen.os.debian :as debian]
            [clojure.java.jdbc :as j]
            [honeysql [core :as sql]
             [helpers :as h]]))

(defn install-hbase!
  "Installs hbase"
  [node]
  (info node "installing hbase + themis")
  (c/su
    (c/cd "/root"
          (let [uri "https://archive.apache.org/dist/hbase/0.98.15/hbase-0.98.15-hadoop2-bin.tar.gz"]
            (when-not (cu/file? "hbase")
              (info node "installing java")
              (debian/install ["openjdk-7-jre-headless"])
              
              (info node "downloading hbase")
              (c/exec :wget :-c uri)
              (c/exec :tar :xzf "hbase-0.98.15-hadoop2-bin.tar.gz")
              (c/exec :mv :-f "hbase-0.98.15-hadoop2" "hbase"))))
    (c/cd "/root/jepsen-tidb-res"
          (info node "installing tso-server")
          (c/exec :tar :xzf "tso-server.tar.gz")
          (c/exec :chmod :+x "tso-server")
          (c/exec :mv :-f "tso-server" "/root")
          (info node "installing themis")
          (c/exec :cp :-f "themis-coprocessor-1.0-SNAPSHOT-jar-with-dependencies.jar" "/root/hbase/lib")
          (c/exec :cp :-f "hbase-site.xml" "/root/hbase/conf")
          (c/exec :cp :-f "hbase-env.sh" "/root/hbase/conf"))))

(defn install-tidb!
  "Installs DB on the given node."
  [node]
  (info node "installing git")
  (debian/install [:git-core])
  (c/su
    (c/cd "/root"
          (when-not (cu/file? "jepsen-tidb-res")
            (info node "cloning jepsen-tidb-res")
            (c/exec :git :clone :--depth 1 "https://github.com/siddontang/jepsen-tidb-res.git")))
    (c/cd "/root/jepsen-tidb-res"
          (info node "pulling latest jepsen-tidb-res")
          (c/exec :git :pull)
          (info node "installing tidb-server")
          (c/exec :tar :xzf "tidb-server.tar.gz")
          (c/exec :chmod :+x "tidb-server")
          (c/exec :mv :-f "tidb-server" "/root"))))

(def store-path (str (name :n1) ":2881|" (name :n1) ":1234/test"))

(def tidb-pidfile "/var/run/tidb-server.pid")
(def tidb-binary "/root/tidb-server")
(def tidb-log "/var/log/tidb-server.log")
(def tso-pidfile "/var/run/tso-server.pid")
(def tso-binary "/root/tso-server")
(def tso-log "/var/log/tso-server.log")

(defn running?
  "Is the service running?"
  [binary pidfile]
  (try
    (c/exec :start-stop-daemon :--status
            :--pidfile pidfile
            :--exec    binary)
    true
    (catch RuntimeException _ false)))

(defn start-tso-server!
  "Starts tso server."
  [node]
  (info node "starting tso server")
  (c/su
    (assert (not (running? tso-binary tso-pidfile)))
    (c/exec :start-stop-daemon :--start
            :--background
            :--make-pidfile
            :--pidfile  tso-pidfile
            :--chdir    "/root"
            :--exec     tso-binary
            :--no-close
            :--
            (str "-addr=:1234")
            :>> tso-log
            (c/lit "2>&1"))
    (info node "tso server started")))

(defn stop-tso-server!
  "Stops tso server."
  [node]
  (info node "stopping tso server")
  (c/su
    (meh (c/exec :killall :-9 :tso-server))
    (meh (c/exec :rm :-rf tso-pidfile))))

(defn start-tidb-server!
  "Starts tidb server."
  [node]
  (info node "starting tidb server")
  (c/su
    (assert (not (running? tidb-binary tidb-pidfile)))
    (c/exec :start-stop-daemon :--start
            :--background
            :--make-pidfile
            :--pidfile  tidb-pidfile
            :--chdir    "/root"
            :--exec     tidb-binary
            :--no-close
            :--
            (str "-L=debug")
            (str "-P=3306")
            (str "-lease=1")
            (str "-store=hbase")
            (str "-path=" (c/lit store-path))
            :>> tidb-log
            (c/lit "2>&1")
            )
    (info node "tidb server started")))

(defn stop-tidb-server!
  "Stops tidb server."
  [node]
  (info node "stopping tidb server")
  (c/su
    (meh (c/exec :killall :-9 :tidb-server))
    (meh (c/exec :rm :-rf tidb-pidfile))))

(defn start-hbase!
  "Starts hbase"
  [node]
  (c/su
    (info node "starting hbase")
    (c/exec "/root/hbase/bin/start-hbase.sh")))

(defn stop-hbase!
  "Stops hbase"
  [node]
  (c/su 
    (info node "stopping hbase")
    ;call stop-hbase.sh is very slow, so here we kill java forcely.
    (meh (cu/grepkill "java"))
    ;(meh (c/exec "/root/hbase/bin/stop-hbase.sh"))
    (meh (c/exec :rm :-rf "/root/hbase/logs/*"))
    (meh (c/exec :rm :-rf "/var/lib/hbase/*"))
    (meh (c/exec :rm :-rf "/var/lib/hbase_zookeeper/*")))
  (info node "hbase stopped"))

(defn db
  "Sets up and tears down TiDB"
  [version]
  (reify db/DB
    (setup! [_ test node]
            (install-tidb! node)
            
            (when (= node :n1)
              (install-hbase! node)
              (start-hbase! node)
              (start-tso-server! node)
              (Thread/sleep 1000))
            
            (jepsen/synchronize test)
            (start-tidb-server! node)
            (info node "set up"))
    
    (teardown! [_ test node]
               (stop-tidb-server! node)
               (when (= node :n1)
                 (stop-tso-server! node)
                 (stop-hbase! node))
               (info node "tore down"))))

(defn basic-test
  "A simple test of MeowDB's safety."
  [version]
  (merge tests/noop-test
         {:os debian/os
          :db (db version)
          :concurrency 20}))


