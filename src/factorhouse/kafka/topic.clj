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

(defn ->category [id name & {:keys [parent props]}]
  (cond-> {:id id
           :name name}
    parent (assoc :parent parent)
    props (merge props)))

(defn- sum-size-of-sizes [sizes]
  (transduce (map :size) + sizes))

(defn- make-category-ctxs [category sizes & {:keys [parent-idxs]
                                             :or {parent-idxs []}}]
  (map-indexed
   (fn [idx [name category-sizes]]
     [idx parent-idxs name category-sizes])
   (->> sizes
        (group-by category)
        (sort-by key))))

(defn- apply-windows [windows sizes]
  (reduce (fn [acc [window-key window-fn]]
            (assoc acc window-key (window-fn sizes)))
          {}
          windows))

(defn- by-categories [category-fns windows sizes]
  (when-let [[first-category-fn] (seq category-fns)]
    (loop [acc []
           [category-ctx & category-ctxs] (make-category-ctxs first-category-fn
                                                              sizes)]
      (if-not category-ctx
        acc
        (let [[idx parent-idxs name category-sizes] category-ctx
              category-fns-cursor (inc (count parent-idxs))
              category-idxs (conj parent-idxs idx)
              category-id (apply ->id category-idxs)
              category-props (apply-windows windows category-sizes)
              category-opts (cond-> {:props category-props}
                              (seq parent-idxs)
                              (assoc :parent (apply ->id parent-idxs)))
              category (->category category-id name category-opts)
              next-category-fn (get category-fns category-fns-cursor)
              next-category-ctxs (if-not next-category-fn
                                   []
                                   (make-category-ctxs
                                    next-category-fn
                                    category-sizes
                                    :parent-idxs category-idxs))]
          (recur (conj acc category)
                 (concat next-category-ctxs
                         category-ctxs)))))))

(defn categories-physical
  "Transform topic sizes into categorised physical view"
  [sizes]
  (by-categories [:broker :topic :partition]
                 {:size sum-size-of-sizes}
                 sizes))

(defn categories-logical
  "Transform topic sizes into categorised logical view"
  [sizes]
  (by-categories [:topic :partition]
                 {:size sum-size-of-sizes}
                 sizes))

