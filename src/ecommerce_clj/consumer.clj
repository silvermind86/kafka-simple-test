(ns ecommerce-clj.consumer
  (:require [ecommerce-clj.kafka :as kafka])
  (:import
   (java.time Duration)
   (java.util Date)))

(defn consume! [configs topic]
  (with-open [consumer (kafka/consumer
                        (kafka/build-properties configs))]
    (.subscribe consumer [topic])
    (loop [records []]
      (if (> (count records) 0)
        (doseq [record records]
          (printf "message-value: %s, message-key: %s, message-offset: %s message-time: %s \n"
                  (.value record)
                  (.key record)
                  (.offset record)
                  (Date. (.timestamp record))))
        (println "Waiting for message in " topic))
      (recur (seq (.poll consumer (Duration/ofSeconds 3)))))))

(defn -main [& args]
  (consume! {:group-id "ecommerce-clj.consumer"
             :client-id (str "consumer-test" (rand-int 500))
             :type :consumer}
            "STORE_NEW_ORDER"))
