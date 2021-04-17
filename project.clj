(defproject ecommerce-clj "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/data.json "2.2.0"]
                 [org.apache.kafka/kafka-clients "2.7.0"]
                 [org.slf4j/slf4j-simple "1.7.30"]]
  :repl-options {:init-ns ecommerce-clj.core}
  :aliases {"produce" ["run" "-m" "ecommerce-clj.producer"]
            "consume" ["run" "-m" "ecommerce-clj.consumer"]})
