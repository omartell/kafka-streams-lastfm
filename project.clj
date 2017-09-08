(defproject kafka-streams-lastfm "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"confluent" "http://packages.confluent.io/maven/"}

  :dependencies [[org.clojure/clojure "1.9.0-alpha17"]
                 [org.apache.kafka/kafka-streams "0.11.0.0"]
                 [org.apache.kafka/kafka-clients "0.11.0.0"]
                 [clj-time "0.14.0"]
                 [org.clojure/data.json "0.2.6"]
                 [cheshire "5.8.0"]
                 [org.clojure/tools.logging "0.4.0"]
                 [ch.qos.logback/logback-classic "1.1.3"]]
  :main ^:skip-aot kafka-streams-lastfm.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
