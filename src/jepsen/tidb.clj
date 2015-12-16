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
          
          (info node "installing interpreter")
          (c/exec :tar :xzf "interpreter.tar.gz")
          (c/exec :chmod :+x "interpreter")
          (c/exec :mv :-f "interpreter" "/root")
          
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

(def store-path (str (name :n1) ":2181|" (name :n1) ":1234/test"))

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
            :> tso-log
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
            (str "-L=error")
            (str "-P=3306")
            ; use a very large lease to prevent crash when network partition.
            ; so we must create all schemas before test.
            (str "-lease=3600")
            (str "-store=hbase")
            (c/lit (str "-path=\"" store-path "\""))
            :> tidb-log
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
    (c/exec :mkdir :-p "/tmp/hbase")
    (meh (c/exec "/root/hbase/bin/start-hbase.sh"))))

(defn stop-hbase!
  "Stops hbase"
  [node]
  (c/su 
    (info node "stopping hbase")
    ; call stop-hbase.sh is very slow, so here we kill java forcely.
    (cu/grepkill "java")
    (Thread/sleep 2000)
    ; (meh (c/exec "/root/hbase/bin/stop-hbase.sh"))
    ;(meh (c/exec :rm :-rf "/var/lib/hbase/"))
    ;(meh (c/exec :rm :-rf "/root/hbase/logs/"))
    (meh (c/exec :rm :-rf "/tmp/hbase/"))
    )
  (info node "hbase stopped"))

(defn eval!
  "Evals a sql string from the command line."
  [s]
  (c/exec :echo s | "/root/interpreter" 
          :-lease 0 
          :-store "hbase" 
          (c/lit (str "-dbpath=\"" store-path "\""))
          :> "/var/log/interpreter.log"))

(defn setup-db!
  "Initialize TiDB database."
  [node]
  (info node "begin to setup tidb database.")
  (eval! (str "create table if not exists dirty
              (id     int not null primary key,
              x       bigint not null);")))


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
              (Thread/sleep 10000)
              (setup-db! node))
            
            (jepsen/synchronize test)
            (start-tidb-server! node)
            
            (jepsen/synchronize test)
            (info node "set up"))
    
    (teardown! [_ test node]
               (stop-tidb-server! node)
               (when (= node :n1)
                 (stop-tso-server! node)
                 (stop-hbase! node)
                 )
               (info node "tore down"))))

(defn conn-spec
  "jdbc connection spec for a node."
  [node]
  {:classname   "org.mariadb.jdbc.Driver"
   :subprotocol "mariadb"
   :subname     (str "//" (name node) ":3306/test")
   :user        "root"
   :password    ""})

; Following transaction helper functions and test clients are copied from jepsen gelera source 
; with a little change.
(def rollback-msg
  "mariadb drivers have a few exception classes that use this message"
  "Deadlock found when trying to get lock; try restarting transaction")

(defmacro capture-txn-abort
  "Converts aborted transactions to an ::abort keyword"
  [& body]
  `(try ~@body
     (catch java.sql.SQLTransactionRollbackException e#
       (if (= (.getMessage e#) rollback-msg)
         ::abort
         (throw e#)))
     (catch java.sql.BatchUpdateException e#
       (if (= (.getMessage e#) rollback-msg)
         ::abort
         (throw e#)))))

(defmacro with-txn-retries
  "Retries body on rollbacks."
  [& body]
  `(loop []
     (let [res# (capture-txn-abort ~@body)]
       (if (= ::abort res#)
         (recur)
         res#))))

(defmacro with-txn-aborts
  "Aborts body on rollbacks."
  [op & body]
  `(let [res# (capture-txn-abort ~@body)]
     (if (= ::abort res#)
       (assoc ~op :type :fail)
       res#)))

(defmacro with-error-handling
  "Common error handling for Galera errors"
  [op & body]
  `(try ~@body
     (catch java.sql.SQLNonTransientConnectionException e#
       (condp = (.getMessage e#)
         "WSREP has not yet prepared node for application use"
         (assoc ~op :type :fail, :value (.getMessage e#))
         
         (throw e#)))))

(defmacro with-txn
  "Executes body in a transaction, with a timeout, automatically retrying
  conflicts and handling common errors."
  [op [c node] & body]
  `(timeout 5000 (assoc ~op :type :info, :value :timed-out)
            (with-error-handling 
              ~op
              (with-txn-retries
                (j/with-db-transaction [~c (conn-spec ~node)
                                        :isolation :serializable]
                                       ~@body)))))


(defrecord DirtyClient [node n]
  client/Client
  (setup! [this test node]
          (j/with-db-connection 
            [c (conn-spec node)]
            (dotimes [i n]
              (try
                (with-txn-retries
                  (Thread/sleep (rand-int 10))
                  (j/insert! c :dirty {:id i, :x -1}))
                (catch java.sql.SQLException e nil))))
          
          (assoc this :node node))
  
  (invoke! [this test op]
           (timeout 5000 (assoc ~op :type :info, :value :timed-out)
                    (with-error-handling 
                      op
                      (with-txn-aborts 
                        op
                        (j/with-db-transaction
                          [c (conn-spec node)
                           :isolation :serializable]
                          (try
                            (case (:f op)
                              :read (->> (j/query c ["select * from dirty"])
                                         (mapv :x)
                                         (assoc op :type :ok, :value))
                              
                              :write (let [x (:value op)
                                           order (shuffle (range n))]
                                       (doseq [i order]
                                         (j/query c ["select * from dirty where id = ?" i]))
                                       (doseq [i order]
                                         (j/update! c :dirty {:x x} ["id = ?" i]))
                                       (assoc op :type :ok)))))))))
  
  (teardown! [_ test]))

(defn dirty-client
  [n]
  (DirtyClient. nil n))

(defn dirty-checker
  "We're looking for a failed transaction whose value became visible to some
  read."
  []
  (reify checker/Checker
    (check [this test model history]
           (let [failed-writes (->> history
                                    (r/filter op/fail?)
                                    (r/filter #(= :write (:f %)))
                                    (r/map :value)
                                    (into (hash-set)))
                 reads (->> history
                            (r/filter op/ok?)
                            (r/filter #(= :read (:f %)))
                            (r/map :value))
                 inconsistent-reads (->> reads
                                         (r/filter (partial apply not=))
                                         (into []))
                 filthy-reads (->> reads
                                   (r/filter (partial some failed-writes))
                                   (into []))]
             {:valid? (empty? filthy-reads)
              :inconsistent-reads inconsistent-reads
              :dirty-reads filthy-reads}))))

(def dirty-reads {:type :invoke, :f :read, :value nil})

(def dirty-writes (->> (range)
                       (map (partial array-map
                                     :type :invoke,
                                     :f :write,
                                     :value))
                       gen/seq))

(defn dirty-test
  [version n]
  (merge tests/noop-test
         {:name "tidb dirty reads"
          :os debian/os
          :db (db version)
          :concurrency 20
          :version version
          :client (dirty-client n)
          :generator (->> (gen/mix [dirty-reads dirty-writes])
                          gen/clients
                          (gen/time-limit 1000))
          :nemesis nemesis/noop
          :checker (checker/compose
                     {:perf (checker/perf)
                      :dirty-reads (dirty-checker)})}))


(defn basic-test
  "A simple test for Tidb."
  [version]
  (merge tests/noop-test
         {:name "tidb"
          :os debian/os
          :db (db version)
          :concurrency 20}))


