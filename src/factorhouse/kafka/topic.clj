(ns factorhouse.kafka.topic)

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

(defn categories-logical
  "Transform topic sizes into categorised logical view"
  [sizes])

(defn categories-physical
  "Transform topic sizes into categorised physical view"
  [sizes])