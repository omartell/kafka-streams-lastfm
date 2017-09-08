(ns kafka-streams-lastfm.core
  (:gen-class)
  (:require [cheshire.core :as cheshire]
            [clj-time.coerce :as time-coerce]
            [clj-time.format :as time-format]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.tools.logging :as log])
  (:import java.util.concurrent.TimeUnit
           [org.apache.kafka.clients.producer KafkaProducer ProducerConfig ProducerRecord]
           [org.apache.kafka.common.serialization Deserializer Serde Serdes Serializer StringDeserializer StringSerializer]
           [org.apache.kafka.streams KafkaStreams KeyValue StreamsConfig]
           [org.apache.kafka.streams.kstream Aggregator Initializer KeyValueMapper KStreamBuilder Predicate Merger SessionWindows ValueMapper]))

(def producer-config
  {ProducerConfig/BOOTSTRAP_SERVERS_CONFIG      "localhost:9092"
   ProducerConfig/ACKS_CONFIG                   "all"
   ProducerConfig/RETRIES_CONFIG                (int 3)
   ProducerConfig/LINGER_MS_CONFIG              (int 100)
   ProducerConfig/BATCH_SIZE_CONFIG             (int 1000)
   ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG   "org.apache.kafka.common.serialization.StringSerializer"
   ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringSerializer"})

(defn produce-play-events [topic filename]
  (log/info "Producing play events into topic" topic "from" filename)
  (with-open [rdr (io/reader filename)]
    (let [producer (KafkaProducer. producer-config)]
      (doseq [line (line-seq rdr)]
        (let [[user-id timestamp artid artname traid traname] (string/split line #"\t")
              parsed-timestamp                                (->> timestamp
                                                                   (time-format/parse (time-format/formatters :date-time-parser))
                                                                   time-coerce/to-long)
              event                                           {:user-id     user-id
                                                               :timestamp   parsed-timestamp
                                                               :artist-id   artid
                                                               :artist-name artname
                                                               :track-id    traid
                                                               :track-name  traname}]
          (.send producer (ProducerRecord. topic nil (:timestamp event) (:user-id event) (json/write-str event)))))
      (.flush producer)
      (.close producer)))
  (log/info "Produced all play events"))

(defrecord JsonSerde []
  Serde
  (configure [_ m b])

  (close [_])

  (serializer [_]
    (let [serializer (StringSerializer.)]
      (reify Serializer
        (configure [_ m b])

        (close [_])

        (serialize [_ s v]
          (.serialize serializer s (cheshire/generate-string v))))))

  (deserializer [_]
    (let [deserializer (StringDeserializer.)]
      (reify Deserializer
        (configure [_ m b])

        (close [_])

        (deserialize [_ ^java.lang.String s #^bytes bs]
          (when-let [v (.deserialize deserializer s bs)]
            (cheshire/parse-string v true)))))))

(defn sset-by-count []
  (sorted-set-by #(> (:count %1) (:count %2))))

(defn smap-by-count []
  (sorted-map-by #(> (:count %1) (:count %2))))

(defn top-tracks [v]
  (->> (mapcat :tracks v)
       (group-by identity)
       (map (fn [[k v]]
              {:track-name k
               :count     (count v)}))
       (sort-by :count #(< %2 %1))
       (take 10)))

(defn top-50-sessions [value aggregate]
  (->> (update (group-by (juxt :user-id :end) aggregate)
               [(:user-id value) (:end value)]
               (fn [[current]]
                 (if (or (nil? current)
                         (< (:count current) (:count value)))
                   [value]
                   [current])))
       vals
       flatten
       (sort-by :count #(< %2 %1))
       (take 50)))

(defn run [app-id input-topic threads]
  (log/info "Generating top track counts from" input-topic "using app-id" app-id "and running" threads "threads")
  (let [props       {StreamsConfig/APPLICATION_ID_CONFIG                        app-id
                     StreamsConfig/BOOTSTRAP_SERVERS_CONFIG                     "localhost:9092"
                     StreamsConfig/NUM_STREAM_THREADS_CONFIG                    (Integer/parseInt threads)
                     StreamsConfig/KEY_SERDE_CLASS_CONFIG                       (.getName (.getClass (Serdes/String)))
                     StreamsConfig/VALUE_SERDE_CLASS_CONFIG                     (.getName (.getClass (Serdes/String)))
                     (StreamsConfig/producerPrefix "max.request.size")          (int 10488610)
                     (StreamsConfig/consumerPrefix "max.partition.fetch.btyes") (int 10488610)}

        config      (StreamsConfig. props)

        builder     (KStreamBuilder.)

        play-events (.stream builder
                             (Serdes/String)
                             (JsonSerde.)
                             (into-array String [input-topic]))

        counts      (-> play-events
                        (.groupByKey)
                        (.aggregate (reify Initializer
                                      (apply [_]
                                        {}))
                                    (reify Aggregator
                                      (apply [_ agg-key value aggregate]
                                        (let [tracks (conj (into #{} (:tracks aggregate))
                                                           (:track-name value))]
                                          {:tracks  tracks
                                           :user-id agg-key
                                           :count   (count tracks)})))
                                    (reify Merger
                                      (apply [_ agg-key lvalue rvalue]
                                        (let [tracks (into (into #{} (:tracks lvalue))
                                                           (:tracks rvalue))]
                                          {:tracks  tracks
                                           :user-id agg-key
                                           :count   (count tracks)})))
                                    (SessionWindows/with (.toMillis (TimeUnit/MINUTES) 20))
                                    (JsonSerde.)
                                    "tracks-by-windowed-session")
                        (.filter (reify Predicate
                                   (test [_ k v]
                                     (< 1000 (:count v)))))
                        (.groupBy (reify KeyValueMapper
                                    (apply [_ k v]
                                      (KeyValue. "all"
                                                 (merge v {:start (.start (.window k))
                                                           :end   (.end (.window k))}))))
                                  (Serdes/String)
                                  (JsonSerde.))
                        (.aggregate (reify Initializer
                                      (apply [_]
                                        []))
                                    (reify Aggregator
                                      (apply [_ agg-key value aggregate]
                                        (log/info "Aggregating" (select-keys value [:user-id :start :end :count]))
                                        (log/info "Aggregate" (map (juxt :user-id :start :end :count) aggregate))
                                        (top-50-sessions value aggregate)))
                                    (reify Aggregator
                                      (apply [_ agg-key value aggregate]
                                        (top-50-sessions value aggregate)))
                                    (JsonSerde.)
                                    "top-sessions-by-tracks")
                        (.mapValues (reify ValueMapper
                                      (apply[_ v]
                                        (log/info "Top tracks" (top-tracks v))
                                        (top-tracks v))))
                        (.toStream)
                        (.to (Serdes/String) (JsonSerde.) (str "top-ten-tracks-" app-id)))
        streams     (KafkaStreams. builder config)]
    (log/info "Staring streams task")
    (.start streams)
    streams))

(defn top-sessions-tsv [top-sessions]
  (spit "top_sessions.tsv" (->> top-sessions
                                (map (fn [[user-id start end count]] [user-id
                                                                      (str (time-coerce/from-long start))
                                                                      (str (time-coerce/from-long end))
                                                                      count]))
                                (into [["user-id" "session-start" "session-end" "tracks-count"]])
                                (map (fn [v] (string/join "\t" v)))
                                (string/join "\n"))))


(defn top-tracks-tsv [top-tracks]
  (spit "top_tracks.tsv" (->> top-tracks
                              (map vals)
                              (into [["track-name" "times-played"]])
                              (map (fn [v] (string/join "\t" v)))
                              (string/join "\n"))))

(defn -main [& args]
  (run))
