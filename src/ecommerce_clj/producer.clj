(ns ecommerce-clj.producer
  (:require [ecommerce-clj.kafka :as kafka]))

(defn produce! [config-fname topic message-key message]
  (let [props (kafka/build-properties config-fname)
        print-ex (comp println (partial str "Failed to deliver message: "))
        print-metadata #(printf "Produced record to topic %s partition [%d] @ offest %d\n"
                                (.topic %)
                                (.partition %)
                                (.offset %))
        create-msg (fn [topic key value]
                     (printf "Producing record: %s\t%s\n" key value)
                     (kafka/producer-record topic key value))]
    (with-open [producer (kafka/producer props)]
      (kafka/create-topic! topic props)
      (.send producer (create-msg topic message-key message)
                      (kafka/generate-callback print-metadata print-ex))
      (.flush producer))))

(defn -main [& args]
  (produce! {:type :producer}
            "STORE_NEW_ORDER"
            "ecommerce"
            "[{\"id\":6,\"nome\":\"home-saves\"}]"))
