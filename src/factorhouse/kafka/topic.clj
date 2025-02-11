(ns factorhouse.kafka.topic
  (:require [clojure.string :as str]))

;; Technical Challenge! Implement this function.

(defn- topics-seq
  "Transform raw topic information into a sequence."
  [topics]
  (for [[broker-id topic] topics
        [dir dir-info] topic
        [topic-and-partition infos] (:replica-infos dir-info)
        :let [{:keys [topic partition]} topic-and-partition
              {:keys [size offset-lag future?]} infos]]
    {:topic topic
     :partition partition
     :size size
     :offset-lag offset-lag
     :future? future?
     :broker broker-id
     :dir dir}))

(defn sizes
  "Transform raw topic information into a sequence sorted by broker, topic, then
   partition."
  [topics]
  (sort-by (juxt :broker :topic :partition)
           (topics-seq topics)))

;; Extension Challenge! Implement these functions.

(defn ->id [& idxs]
  (str/join "." idxs))

(defn ->category [id name size & {:keys [parent]}]
  (cond-> {:id id
           :name name
           :size size}
    parent (assoc :parent parent)))

(defn sum-size-of-sizes [sizes]
  (transduce (map :size) + sizes))

(defn categories-physical
  "Transform topic sizes into categorised physical view"
  [sizes]
  (let [by-broker (group-by :broker sizes)
        by-broker-and-topic (update-vals by-broker #(group-by :topic %))
        by-broker-and-topic-and-partition
        (update-vals by-broker-and-topic
                     (fn [by-topic]
                       (update-vals by-topic
                                    #(group-by :partition %))))]
    (->> (sort-by key by-broker)
         (map-indexed #(into [%1] %2))
         (mapcat
          (fn [[i broker broker-sizes]]
            (conj (->> (sort-by key (by-broker-and-topic broker))
                       (map-indexed #(into [%1] %2))
                       (mapcat
                        (fn [[j topic topic-sizes]]
                          (conj (->> (sort-by key (get-in by-broker-and-topic-and-partition
                                                          [broker topic]))
                                     (map-indexed #(into [%1] %2))
                                     (map (fn [[k partition partition-sizes]]
                                            (->category (->id i j k)
                                                        partition
                                                        (sum-size-of-sizes partition-sizes)
                                                        :parent (->id i j)))))
                                (->category (->id i j)
                                            topic
                                            (sum-size-of-sizes topic-sizes)
                                            :parent (->id i))))))
                  (->category (->id i)
                              broker
                              (sum-size-of-sizes broker-sizes))))))))

(defn categories-logical
  "Transform topic sizes into categorised logical view"
  [sizes]
  (let [by-topic (group-by :topic sizes)
        by-topic-and-partition (update-vals by-topic #(group-by :partition %))]
    (->> (sort-by key by-topic)
         (map-indexed #(into [%1] %2))
         (mapcat (fn [[i topic topic-sizes]]
                   (conj (map-indexed
                          (fn [j [partition t-and-p-sizes]]
                            (->category (->id i j)
                                        partition
                                        (sum-size-of-sizes t-and-p-sizes)
                                        :parent (->id i)))
                          (sort-by key (by-topic-and-partition topic)))
                         (->category (->id i)
                                     topic
                                     (sum-size-of-sizes topic-sizes))))))))

