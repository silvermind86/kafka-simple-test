(ns ecommerce-clj.consumer
  (:import
   (java.time Duration)
   (java.util Properties Date)
   (org.apache.kafka.clients.consumer ConsumerConfig KafkaConsumer)))

(defn- build-properties [{:keys [brooker group-id deserilalize-key deserialize-value client-id]
                          :or {brooker  "127.0.0.1:9092"
                               group-id "default"
                               deserilalize-key "org.apache.kafka.common.serialization.StringDeserializer"
                               deserialize-value "org.apache.kafka.common.serialization.StringDeserializer"
                               client-id "default-consumer"}}]
  (doto (Properties.)
    (.putAll {ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG brooker
              ConsumerConfig/GROUP_ID_CONFIG group-id
              ConsumerConfig/CLIENT_ID_CONFIG client-id
              ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG deserilalize-key
              ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG deserialize-value})))

(defn consume! [configs topic]
  (with-open [consumer (KafkaConsumer. (build-properties configs))]
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
             :client-id "consumer-test"}
            "STORE_NEW_ORDER"))
