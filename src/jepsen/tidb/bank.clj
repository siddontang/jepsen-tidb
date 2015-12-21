(ns jepsen.tidb.bank
  "Bank transfer test for TiDB.
  
  In this test, writers compete to set every row in a table to some unique
  value. Concurrently, readers attempt to read every row. We're looking for
  casdes where a *failed* transaction's number was visible to some reader."
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
             [util :refer [timeout meh]]
             [tidb :as tidb]]
            [jepsen.control.util :as cu]
            [jepsen.control.net :as cn]
            [jepsen.os.debian :as debian]
            [clojure.java.jdbc :as j]))


(defrecord BankClient [node n starting-balance]
  client/Client
  (setup! [this test node]
          (j/with-db-connection [c (tidb/conn-spec node)]
                                ; Create initial accts
                                (dotimes [i n]
                                  (try
                                    (tidb/with-txn-retries
                                      (j/insert! c :accounts {:id i, :balance starting-balance}))
                                    (catch java.sql.SQLException e nil))))
          
          (assoc this :node node))
  
  (invoke! [this test op]
           (tidb/with-txn op [c node]
                          (try
                            (case (:f op)
                              :read (->> (j/query c ["select * from accounts"])
                                         (mapv :balance)
                                         (assoc op :type :ok, :value))
                              
                              :transfer
                              (let [{:keys [from to amount]} (:value op)
                                    b1 (-> c
                                           (j/query ["select * from accounts where id = ?" from]
                                                    :row-fn :balance)
                                           first
                                           (- amount))
                                    b2 (-> c
                                           (j/query ["select * from accounts where id = ?" to]
                                                    :row-fn :balance)
                                           first
                                           (+ amount))]
                                (cond (neg? b1)
                                      (assoc op :type :fail, :value [:negative from b1])
                                      
                                      (neg? b2)
                                      (assoc op :type :fail, :value [:negative to b2])
                                      
                                      true
                                      (do (j/update! c :accounts {:balance b1} ["id = ?" from])
                                        (j/update! c :accounts {:balance b2} ["id = ?" to])
                                        (assoc op :type :ok))))))))
  
  (teardown! [_ test]))

(defn bank-client
  "Simulates bank account transfers between n accounts, each starting with
  starting-balance."
  [n starting-balance]
  (BankClient. nil n starting-balance))

(defn bank-read
  "Reads the current state of all accounts without any synchronization."
  [_ _]
  {:type :invoke, :f :read})

(defn bank-transfer
  "Transfers a random amount between two randomly selected accounts."
  [test process]
  (let [n (-> test :client :n)]
    {:type  :invoke
     :f     :transfer
     :value {:from   (rand-int n)
             :to     (rand-int n)
             :amount (rand-int 5)}}))

(def bank-diff-transfer
  "Like transfer, but only transfers between *different* accounts."
  (gen/filter (fn [op] (not= (-> op :value :from)
                             (-> op :value :to)))
              bank-transfer))

(defn bank-checker
  "Balances must all be non-negative and sum to the model's total."
  []
  (reify checker/Checker
    (check [this test model history]
           (let [bad-reads (->> history
                                (r/filter op/ok?)
                                (r/filter #(= :read (:f %)))
                                (r/map (fn [op]
                                         (let [balances (:value op)]
                                           (cond (not= (:n model) (count balances))
                                                 {:type :wrong-n
                                                  :expected (:n model)
                                                  :found    (count balances)
                                                  :op       op}
                                                 
                                                 (not= (:total model)
                                                       (reduce + balances))
                                                 {:type :wrong-total
                                                  :expected (:total model)
                                                  :found    (reduce + balances)
                                                  :op       op}))))
                                (r/filter identity)
                                (into []))]
             {:valid? (empty? bad-reads)
              :bad-reads bad-reads}))))

(defn bank-test
  [version n initial-balance]
  (merge tests/noop-test
         {:name "tidb bank reads"
          :os debian/os
          :db (tidb/db version)
          :concurrency 20
          :version version
          :model  {:n n :total (* n initial-balance)}
          :client (bank-client n initial-balance)
          :generator (gen/phases
                       (->> (gen/mix [bank-read bank-diff-transfer])
                            (gen/clients)
                            (gen/stagger 1/10)
                            (gen/time-limit 100))
                       (gen/log "waiting for quiescence")
                       (gen/sleep 30)
                       (gen/clients (gen/once bank-read)))
          :nemesis nemesis/noop
          :checker (checker/compose
                     {:perf (checker/perf)
                      :bank (bank-checker)})}))