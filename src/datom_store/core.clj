(ns datom-store.core
  (:require [clojure.java.io :as io])
  (:import [com.sleepycat.je DatabaseException DatabaseEntry LockMode CacheMode]
           [com.sleepycat.je CheckpointConfig StatsConfig VerifyConfig]
           [com.sleepycat.je Environment EnvironmentConfig EnvironmentMutableConfig]
           [com.sleepycat.je Transaction TransactionConfig]
           [com.sleepycat.je Database DatabaseConfig]
           [com.sleepycat.je Cursor SecondaryCursor JoinCursor CursorConfig JoinConfig]
           [com.sleepycat.je SecondaryDatabase SecondaryConfig SecondaryKeyCreator]
           [com.sleepycat.je DatabaseEntry OperationStatus]
           [com.sleepycat.bind.tuple TupleBinding TupleInput TupleOutput]
           [java.util Comparator]))


;; ===== serialization


;; using pr-str for now
;; but could/sholud use something faster (nippy)
(defn datom-tuple [datom]
  (let [tuple-output (TupleOutput.)]
    (.writeString tuple-output (pr-str datom))))


(defn datom-tuple->db-entry [dt]
  (let [de (DatabaseEntry.)]
    (TupleBinding/outputToEntry dt de)
    de))


(defn read-db-entry [de]
  (let [tuple-input (TupleBinding/entryToInput de)]
    (read-string
      (.readString tuple-input))))


(defn write-db-entry [datom]
  (datom-tuple->db-entry
    (datom-tuple datom)))


;; ===== setup


(defn compare-rows [a b]
  (let [e1 (DatabaseEntry. a)
        e2 (DatabaseEntry. b)
        v1 (read-db-entry e1)
        v2 (read-db-entry e2)
        cmp-result (compare
                     (or (get v1 0) 0)
                     (or (get v2 0) 0))]
    cmp-result))


(deftype DatomComparator []
  java.util.Comparator
  (compare [this a b]
    (compare-rows a b))
  (equals [a b]
    (= a b)))


(def env
  (Environment.
    (io/file "./bdb")
    (doto (EnvironmentConfig.)
      (.setAllowCreate true)
      (.setTransactional true))))


(def datom-comparator
  (comparator compare-rows))


(def db-config
  (doto (DatabaseConfig.)
    (.setTransactional true)
    (.setAllowCreate true)
    (.setOverrideBtreeComparator true)
    (.setBtreeComparator DatomComparator)))


(def database
  (.openDatabase
   env
   nil
   "test3"
   db-config))


(defn db-get [db k]
  (let [result-entry (DatabaseEntry.)
        get-result (.get db
                         nil
                         (write-db-entry k)
                         result-entry
                         LockMode/DEFAULT)]
    (read-db-entry result-entry)))


(defn db-put! [db k]
  (.put
    db
    nil
    (write-db-entry k)
    (write-db-entry k)))


;; ===== cursors

(defn cursor-next [cursor k]
  (let [next-entry (DatabaseEntry.)]
    (println
      ".getNext res"
      (.getNext
        cursor
        (write-db-entry k)
        next-entry
        LockMode/DEFAULT))
    (read-db-entry next-entry)))


(defn cursor-search-key [cursor k]
  (let [result-entry (DatabaseEntry.)]
    (.getSearchKey
      cursor
      (write-db-entry k)
      result-entry
      LockMode/DEFAULT)
    (read-db-entry result-entry)))


;; create a cursor starting at k
(defn search-cursor 
  ([k]
   (let [cursor (.openCursor database nil nil)]
     (search-cursor 
       cursor
       (cursor-search-key cursor k))))
  ([cursor v]
   (lazy-seq
     (cons v 
           (search-cursor 
             cursor
             (cursor-next cursor v))))))


;; ===== scratch

(comment
  (db-put! database [124 :user/foo "pizza"]))

(defn test1 []
  (type
    (db-get database [124 :user/foo "pizza"])))



(comment
  (time
    (doall
      (take 4
       (search-cursor [4 :user/likes "pizza"])))))


(comment
  (time
    (doseq [x (range 50)]
      (println
        (db-put! database [x :user/likes "pizza"])))))


(comment
  (time
    (.count database)))


(comment
  (.getStats database
             (StatsConfig.)))

;
