(ns jepsen.tidb-test
  (:require [clojure.test :refer :all]
            [jepsen.core :refer [run!]]
            [jepsen.tidb :as tidb]
            [jepsen.tidb.bank :as bank]
            [jepsen.tidb.dirty-reads :as dirty-reads]))

(def version 
  "What TiDB version should we test?"
  "0.0.1")

; (deftest basic-test
;   (is (:valid? (:results (run! (tidb/basic-test version))))))

(deftest bank-test
     (is (:valid? (:results (run! (bank/bank-test version 2 10))))))

 ; (deftest dirty-reads-test
 ;   (is (:valid? (:results (run! (dirty-reads/dirty-test version 4))))))