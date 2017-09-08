(ns kafka-streams-lastfm.tsv
  (:require [clojure.string :as string]
            [clj-time.coerce :as time-coerce]))

(defn top-sessions [top-sessions]
  (spit "top_sessions.tsv" (->> top-sessions
                                (map (fn [[user-id start end count]] [user-id
                                                                      (str (time-coerce/from-long start))
                                                                      (str (time-coerce/from-long end))
                                                                      count]))
                                (into [["user-id" "session-start" "session-end" "tracks-count"]])
                                (map (fn [v] (string/join "\t" v)))
                                (string/join "\n"))))


(defn top-tracks [top-tracks]
  (spit "top_tracks.tsv" (->> top-tracks
                              (map vals)
                              (into [["track-name" "times-played"]])
                              (map (fn [v] (string/join "\t" v)))
                              (string/join "\n"))))
