(ns ecommerce-clj.producer  
  (:import
   (java.util Properties)
   (org.apache.kafka.clients.admin AdminClient NewTopic)
   (org.apache.kafka.clients.producer Callback KafkaProducer ProducerConfig ProducerRecord)
   (org.apache.kafka.common.errors TopicExistsException)))

(defn- build-properties [config-map]
  (doto (Properties.)
    (.putAll config-map)))

(defn- create-topic! [topic partitions replication cloud-config]
  (let [ac (AdminClient/create cloud-config)]
    (try
      (.createTopics ac [(NewTopic. topic (int partitions) (short replication))])
      (catch TopicExistsException e nil)
      (finally
        (.close ac)))))

(defn produce! [config-fname topic message-key message]
  (let [props (build-properties config-fname)
        print-ex (comp println (partial str "Failed to deliver message: "))
        print-metadata #(printf "Produced record to topic %s partition [%d] @ offest %d\n"
                                (.topic %)
                                (.partition %)
                                (.offset %))
        create-msg (fn [topic key value]
                     (printf "Producing record: %s\t%s\n" key value)
                     (ProducerRecord. topic key value))]
    (with-open [producer (KafkaProducer. props)]
      (create-topic! topic 1 3 props)
      (let [callback (reify Callback
                       (onCompletion [this metadata exception]
                         (if exception
                           (print-ex exception)
                           (print-metadata metadata))))]
        (.send producer (create-msg topic message-key message) callback)
        (.flush producer)))))

(defn -main [& args]
  (produce! {ProducerConfig/BOOTSTRAP_SERVERS_CONFIG "127.0.0.1:9092"
             ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringSerializer"
             ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringSerializer"}
            "LOJA_NOVO_PEDIDO"
            "ecommerce"
            "[{\"id\":6,\"nome\":\"home-saves\"}]"))
