(ns factorhouse.kafka.topic-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [factorhouse.kafka.topic :as topic]
            [factorhouse.test.data :as data]))

(def ^:dynamic *num-prop-tests* 100)

(def ^:dynamic *topic-id-gen* gen/string)

(def ^:dynamic *partition-id-gen* gen/nat)

(def ^:dynamic *replica-info-key-gen*
  (gen/hash-map
   :topic *topic-id-gen*
   :partition *partition-id-gen*))

(def ^:dynamic *size-val-gen* gen/nat)

(def ^:dynamic *offset-lag-gen* gen/nat)

(def ^:dynamic *future?-gen* gen/boolean)

(def ^:dynamic *replica-info-val-gen*
  (gen/hash-map
   :size *size-val-gen*
   :offset-lag *offset-lag-gen*
   :future? *future?-gen*))

(def ^:dynamic *replica-info-gen*
  (gen/map *replica-info-key-gen*
           *replica-info-val-gen*))

(def ^:dynamic *error-gen* (gen/return "NONE"))

(def ^:dynamic *dir-gen*
  (gen/hash-map
   :error *error-gen*
   :replica-infos *replica-info-gen*))

(def ^:dynamic *dir-path-gen* gen/string)

(def ^:dynamic *brokers-gen*
  (gen/map *dir-path-gen* *dir-gen*))

(def ^:dynamic *broker-id-gen* gen/nat)

(def ^:dynamic *topics-gen*
  (gen/map *broker-id-gen* *brokers-gen*))

(def ^:dynamic *size-gen*
  (gen/hash-map
   :topic *topic-id-gen*
   :partition *partition-id-gen*
   :size *size-val-gen*
   :offset-lag *offset-lag-gen*
   :future? *future?-gen*
   :broker *broker-id-gen*
   :dir *dir-path-gen*))

(def id->ints (comp #(mapv Integer/parseInt %) #(str/split % #"\.")))

(defn right-pad [n pad-with seq]
  (into seq (repeat (- n (count seq)) pad-with)))

(defn valid-id? [id]
  (boolean (re-matches #"\d+(\.\d+)*" id)))

(defn valid-size? [size]
  (and
   (nat-int? (:broker size))
   (string? (:topic size))
   (nat-int? (:partition size))
   (string? (:dir size))
   (nat-int? (:size size))
   (nat-int? (:offset-lag size))
   (boolean? (:future? size))))

(defn valid-category? [{:keys [id size parent]}]
  (and
   (valid-id? id)
   (nat-int? size)
   (or (nil? parent) (valid-id? parent))))

(defn sub-id
  ([id start]
   (apply topic/->id (subvec (id->ints id) start)))
  ([id start end]
   (apply topic/->id (subvec (id->ints id) start end))))

(defn sum-size-of-sizes-by [categories sizes]
  (update-vals (group-by (apply juxt categories) sizes)
               topic/sum-size-of-sizes))

(def sizes-count-prop
  "The count of returned sizes should be equal to the count of all the
   replica-info in the provided topics map."
  (prop/for-all [topics (gen/resize 10 *topics-gen*)]
                (let [all-replica-infos (for [[_ topic] topics
                                              [_ dir] topic
                                              replica-info (:replica-infos dir)]
                                          replica-info)]
                  (= (count all-replica-infos)
                     (count (topic/sizes topics))))))

(deftest sizes-count
  (let [prop sizes-count-prop
        check (tc/quick-check *num-prop-tests* prop)]
    (is (:pass? check) (str "Failed with seed: " (:seed check)))))

(def sizes-order-prop
  "The order of returned sizes should be by broker, then topic, then partition."
  (prop/for-all [topics (gen/resize 10 *topics-gen*)] 
                (let [broker-topic-partition-triplets
                      (for [[broker topic] topics
                            [_ dir] topic
                            [topic-and-partition _] (:replica-infos dir)
                            :let [{:keys [topic partition]} topic-and-partition]]
                        {:broker broker
                         :topic topic
                         :partition partition})]
                  (= (map #(select-keys % [:broker :topic :partition])
                          (topic/sizes topics))
                     (sort-by (juxt :broker :topic :partition)
                              broker-topic-partition-triplets)))))

(deftest sizes-order
  (let [prop sizes-order-prop
        check (tc/quick-check *num-prop-tests* prop)]
    (is (:pass? check) (str "Failed with seed: " (:seed check)))))

(def sizes-structure-prop
  "All returned sizes should be shaped like a size."
  (prop/for-all [topics (gen/resize 10 *topics-gen*)]
    (let [sizes (topic/sizes topics)]
      (every? valid-size? sizes))))

(deftest sizes-structure
  (let [prop sizes-structure-prop
        check (tc/quick-check *num-prop-tests* prop)]
    (is (:pass? check) (str "Failed with seed: " (:seed check)))))

(def categories-physical-count-prop
  "The count of returned categories should be equal to the count of all brokers,
   plus the count of all broker-topic pairs, plus the count of all
   broker-topic-partition triplets."
  (prop/for-all [sizes (gen/resize 100 (gen/vector *size-gen*))]
    (let [brokers (set (map :broker sizes))
          broker-and-topics (set (map (juxt :broker :topic) sizes))
          broker-and-topics-and-partitions (set (map (juxt :broker
                                                           :topic
                                                           :partition)
                                                     sizes))]
      (= (count (topic/categories-physical sizes))
         (+ (count brokers)
            (count broker-and-topics)
            (count broker-and-topics-and-partitions))))))

(deftest categories-physical-count
  (let [prop categories-physical-count-prop
        check (tc/quick-check *num-prop-tests* prop)]
    (is (:pass? check) (str "Failed with seed: " (:seed check)))))

(def categories-physical-order-prop
  "The order of returned sizes should be by id's integer components."
  (prop/for-all [sizes (gen/resize 100 (gen/vector *size-gen*))]
    (let [categories (topic/categories-physical sizes)]
      (= categories (sort-by (comp #(right-pad 3 nil %) id->ints :id)
                             categories)))))

(deftest categories-physical-order
  (let [prop categories-physical-order-prop
        check (tc/quick-check *num-prop-tests* prop)]
    (is (:pass? check) (str "Failed with seed: " (:seed check)))))

(def categories-physical-structure-prop
  "All returned categories should be shaped like a category."
  (prop/for-all [sizes (gen/resize 100 (gen/vector *size-gen*))]
    (let [categories (topic/categories-physical sizes)]
      (every? valid-category? categories))))

(deftest categories-physical-structure
  (let [prop categories-physical-structure-prop
        check (tc/quick-check *num-prop-tests* prop)]
    (is (:pass? check) (str "Failed with seed: " (:seed check)))))

(def categories-physical-size-prop
  "The size of all returned categories should be equal to the sum of the sizes
   of the corresponding broker, broker-topic pair or broker-topic-parition
   triplet."
  (prop/for-all [sizes (gen/resize 100 (gen/vector *size-gen*))]
    (let [names->broker-size (sum-size-of-sizes-by [:broker] sizes)
          names->topic-size (sum-size-of-sizes-by [:broker :topic] sizes)
          names->partition-size (sum-size-of-sizes-by
                                 [:broker :topic :partition]
                                 sizes)
          categories (topic/categories-physical sizes)
          broker-id->names (->> categories
                                (filter #(= 1 (count (id->ints (:id %)))))
                                (map (juxt :id (comp vector :name)))
                                (into {}))
          topic-id->names (->> categories
                               (filter #(= 2 (count (id->ints (:id %)))))
                               (map (juxt :id #(conj (broker-id->names
                                                      (sub-id (:id %) 0 1))
                                                     (:name %))))
                               (into {}))
          partition-id->names (->> categories
                                   (filter #(= 3 (count (id->ints (:id %)))))
                                   (map (juxt :id #(conj (topic-id->names
                                                          (sub-id (:id %) 0 2))
                                                         (:name %))))
                                   (into {}))
          ids->sizes (merge
                      (update-vals broker-id->names names->broker-size)
                      (update-vals topic-id->names names->topic-size)
                      (update-vals partition-id->names names->partition-size))]
      (every? #(= (:size %) (ids->sizes (:id %))) categories))))

(deftest categories-physical-size
  (let [prop categories-physical-size-prop
        check (tc/quick-check *num-prop-tests* prop)]
    (is (:pass? check) (str "Failed with seed: " (:seed check)))))

(def categories-logical-count-prop
  "The count of returned categories should be equal to the count of all topics,
   plus the count of all topic-partition pairs."
  (prop/for-all [sizes (gen/resize 100 (gen/vector *size-gen*))]
                (let [topics (set (map :topic sizes))
                      topic-and-partitions (set (map (juxt :topic :partition)
                                                     sizes))]
                  (= (count (topic/categories-logical sizes))
                     (+ (count topics) (count topic-and-partitions))))))

(deftest categories-logical-count
  (let [prop categories-logical-count-prop
        check (tc/quick-check *num-prop-tests* prop)]
    (is (:pass? check) (str "Failed with seed: " (:seed check)))))

(def categories-logical-order-prop
  "The order of returned sizes should be by id's integer components."
  (prop/for-all [sizes (gen/resize 100 (gen/vector *size-gen*))]
                (let [categories (topic/categories-logical sizes)]
                  (= categories (sort-by (comp #(right-pad 3 nil %) id->ints :id)
                                         categories)))))

(deftest categories-logical-order
  (let [prop categories-logical-order-prop
        check (tc/quick-check *num-prop-tests* prop)]
    (is (:pass? check) (str "Failed with seed: " (:seed check)))))

(def categories-logical-structure-prop
  "All returned categories should be shaped like a category."
  (prop/for-all [sizes (gen/resize 100 (gen/vector *size-gen*))]
                (let [categories (topic/categories-logical sizes)]
                  (every? valid-category? categories))))

(deftest categories-logical-structure
  (let [prop categories-logical-structure-prop
        check (tc/quick-check *num-prop-tests* prop)]
    (is (:pass? check) (str "Failed with seed: " (:seed check)))))

(def categories-logical-size-prop
  "The size of all returned categories should be equal to the sum of the sizes
   of the corresponding topic or topic-partition pair"
  (prop/for-all [sizes (gen/resize 100 (gen/vector *size-gen*))]
                (let [names->topic-size (sum-size-of-sizes-by [:topic] sizes)
                      names->partition-size (sum-size-of-sizes-by
                                             [:topic :partition]
                                             sizes)
                      categories (topic/categories-logical sizes)
                      topic-id->names (->> categories
                                           (filter #(= 1 (count (id->ints (:id %)))))
                                           (map (juxt :id (comp vector :name)))
                                           (into {}))
                      partition-id->names (->> categories
                                               (filter #(= 2 (count (id->ints (:id %)))))
                                               (map (juxt :id #(conj (topic-id->names
                                                                      (sub-id (:id %) 0 1))
                                                                     (:name %))))
                                               (into {}))
                      ids->sizes (merge
                                  (update-vals topic-id->names names->topic-size)
                                  (update-vals partition-id->names names->partition-size))]
                  (every? #(= (:size %) (ids->sizes (:id %))) categories))))

(deftest categories-logical-size
  (let [prop categories-logical-size-prop
        check (tc/quick-check *num-prop-tests* prop)]
    (is (:pass? check) (str "Failed with seed: " (:seed check)))))

(deftest sizes
  (is (= data/sizes
         (topic/sizes data/topics))))

(deftest categories
  (is (= data/categories-physical
         (topic/categories-physical data/sizes)))

  (is (= data/categories-logical
         (topic/categories-logical data/sizes))))