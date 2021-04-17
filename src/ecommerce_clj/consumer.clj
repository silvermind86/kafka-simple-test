(ns ecommerce-clj.consumer
  (:import
   (java.time Duration)
   (java.util Properties Date)
   (org.apache.kafka.clients.consumer ConsumerConfig KafkaConsumer)))

(defn- build-properties [config-map]
  (doto (Properties.)
    (.putAll config-map)))

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
  (consume! {ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG "127.0.0.1:9092"
             ConsumerConfig/GROUP_ID_CONFIG "ecommerce-clj.consumer"
             ConsumerConfig/CLIENT_ID_CONFIG "consumer-test"
             ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringDeserializer"
             ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringDeserializer"}
            "LOJA_NOVO_PEDIDO"))
