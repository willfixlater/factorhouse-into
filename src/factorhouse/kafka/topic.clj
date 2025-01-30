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

(defn categories-physical
  "Transform topic sizes into categorised physical view"
  [sizes]
  (mapcat identity
          (map-indexed (fn [i xs]
                         (into
                          [{:id (->id i)
                            :name (:broker (first xs))
                            :size (transduce (map :size) + xs)}]
                          (mapcat identity)
                          (map-indexed (fn [j ys]
                                         (into
                                          [{:id (->id i j)
                                            :name (:topic (first ys))
                                            :parent (->id i)
                                            :size (transduce (map :size) + ys)}]
                                          (map-indexed (fn [k zs]
                                                         {:id (->id i j k)
                                                          :name (:partition (first zs))
                                                          :parent (->id i j)
                                                          :size (transduce (map :size) + zs)})
                                                       (partition-by :partition ys))))
                                       (partition-by :topic xs))))
                       (partition-by :broker sizes))))

(defn categories-logical
  "Transform topic sizes into categorised logical view"
  [sizes]
  (let [by-broker (partition-by :broker sizes)
        by-broker-by-topic (map #(partition-by :topic %) by-broker)
        topic->partition-sizes (apply mapcat
                                      (fn [& by-topic]
                                        (apply map
                                               (fn [& xs]
                                                 (reduce (fn [a b]
                                                           (update a :size
                                                                   + (:size b)))
                                                         xs))
                                               by-topic))
                                      by-broker-by-topic)]
    (mapcat identity
            (map-indexed (fn [i xs]
                           (into
                            [{:id (->id i)
                              :name (:topic (first xs))
                              :size (transduce (map :size) + xs)}]
                            (mapcat identity)
                            (map-indexed (fn [j ys]
                                           (into
                                            [{:id (->id i j)
                                              :name (:partition (first ys))
                                              :parent (->id i)
                                              :size (transduce (map :size) + ys)}]))
                                         (partition-by :partition xs))))
                         (partition-by :topic topic->partition-sizes)))))