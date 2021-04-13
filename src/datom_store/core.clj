(ns datom-store.core
  ; (:refer-clojure :exlude [get]) 
  (:require [clojure.java.io :as io]
            [datascript.core :as ds])
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



(def datom-comparator
  (comparator compare-rows))


(defn make-env [file-path]
  (Environment.
    (io/file file-path)
    (doto (EnvironmentConfig.)
      (.setAllowCreate true)
      (.setTransactional true))))


(defn make-db-config [_]
  (doto (DatabaseConfig.)
    ; (.setTransactional true)
    (.setAllowCreate true)
    (.setOverrideBtreeComparator true)
    (.setBtreeComparator DatomComparator)
    (.setDeferredWrite true)))


(defn open-db [env {:keys [db-name]}]
  (.openDatabase
   env
   nil
   db-name
   ;; TODO 
   ;; pass config
   (make-db-config nil)))


;; TODO
;; handle operation status
(defn get [db k]
  (let [result-entry (DatabaseEntry.)
        get-result (.get db
                         nil
                         (write-db-entry k)
                         result-entry
                         LockMode/DEFAULT)]
    (read-db-entry result-entry)))


;; TODO
;; handle operation status
(defn put! [bdb k]
  (.put
    bdb
    nil
    (write-db-entry k)
    (write-db-entry k)))


(defn delete! [bdb k]
  (.delete bdb
           nil
           (write-db-entry k)))


;; ===== cursors

;; TODO handle .getNext
;; operation result
(defn cursor-next [cursor k]
  (let [next-entry (DatabaseEntry.)]
    (.getNext
      cursor
      (write-db-entry k)
      next-entry
      LockMode/DEFAULT)
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
  ([bdb k]
   (let [cursor (.openCursor bdb nil nil)]
     (search-cursor 
       bdb
       cursor
       (cursor-search-key cursor k))))
  ([bdb cursor v]
   (lazy-seq
     (cons v 
           (search-cursor 
             bdb
             cursor
             (cursor-next cursor v))))))


(defn sync! [bdb]
  (.sync bdb))


(defn stats [bdb]
  (let [stats-res (.getStats bdb (StatsConfig.))]
    (.toString stats-res)))
    ; {:leaf-node-count (.getLeafNodeCount stats-res)
    ;  :main-tree-max-depth (.getMainTreeMaxDepth stats-res)}))

;
