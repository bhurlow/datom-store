(ns datom-store.benchmark
  (:require [datom-store.core :as ds]))


(set! *print-length* 10)

(def bdb-env (ds/make-env "./bdb"))

(def bdb (ds/open-db bdb-env {:db-name "test4"}))


(ds/put! bdb [1 :my/attr "sup"])

(ds/get bdb [1 :my/attr "sup"])

(defn load-data []
  (doseq [x (range 5000)]
    (println
      (ds/put! bdb [x :user/likes "pizza"])))
  (ds/sync! bdb))

(println
  (ds/stats bdb))

(comment
  (count
    (time
      (take 500
            (ds/search-cursor bdb [5001 :user/likes "pizza"])))))
  



