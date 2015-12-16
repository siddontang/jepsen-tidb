(ns jepsen.tidb-test
  (:require [clojure.test :refer :all]
            [jepsen.core :refer [run!]]
            [jepsen.tidb :as tidb]))

(def version 
  "What TiDB version should we test?"
  "0.0.1")

; (deftest basic-test
;   (is (:valid? (:results (run! (tidb/basic-test version))))))


(deftest dirty-reads-test
  (is (:valid? (:results (run! (tidb/dirty-test version 4))))))