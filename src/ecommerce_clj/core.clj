(ns ecommerce-clj.core
  (:gen-class)
  (:require
   [ecommerce-clj.producer :as producer])
  (:import
   (org.apache.kafka.clients.producer ProducerConfig)
   (org.apache.kafka.common.serialization StringSerializer)))
