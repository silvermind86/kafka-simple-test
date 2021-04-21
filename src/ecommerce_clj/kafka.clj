(ns ecommerce-clj.kafka
  (:import
    (java.time Duration)
    (java.util Properties Date)
    (org.apache.kafka.clients.admin AdminClient NewTopic)
    (org.apache.kafka.clients.producer Callback KafkaProducer ProducerConfig ProducerRecord)
    (org.apache.kafka.clients.consumer ConsumerConfig KafkaConsumer)
    (org.apache.kafka.common.errors TopicExistsException)))

(defn build-properties [{:keys [brooker group-id type key-parser value-parser client-id]
                          :or {brooker  "127.0.0.1:9092"
                               group-id "default"
                               type :consumer
                               client-id "default-consumer"}
                          :as config-map}]
    (let [serialization (if (= :producer type)
                          {ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG (or key-parser
                                                                          "org.apache.kafka.common.serialization.StringSerializer")
                           ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG (or value-parser
                                                                            "org.apache.kafka.common.serialization.StringSerializer")}
                          {ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG (or key-parser
                                                                            "org.apache.kafka.common.serialization.StringDeserializer")
                           ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG (or value-parser
                                                                              "org.apache.kafka.common.serialization.StringDeserializer")
                           ConsumerConfig/GROUP_ID_CONFIG group-id
                           ConsumerConfig/CLIENT_ID_CONFIG client-id})]
    (doto (Properties.)
      (.putAll (merge
                {ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG brooker}
                 serialization)))))

(defn create-topic! [topic {:keys [partitions replication]
                            :or {partitions  1
                                 replication 3}
                            :as config}]
 (let [ac (AdminClient/create config)]
   (try
     (.createTopics ac [(NewTopic. topic (int partitions) (short replication))])
     (catch TopicExistsException e nil)
     (finally
       (.close ac)))))

(defn consumer [configs]
  (KafkaConsumer. configs))

(defn producer-record [topic key value]
  (ProducerRecord. topic key value))

(defn generate-callback [data-handler exception-handler]
  (reify Callback
   (onCompletion [this metadata exception]
     (if exception
       (exception-handler exception)
       (data-handler metadata)))))

(defn producer [configs]
  (KafkaProducer. configs))
