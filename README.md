# kafka-streams-lastfm

Collecting top track counts using [Kafka Streams](http://docs.confluent.io/current/streams/index.html)

## Dependencies

- Kafka 0.11.0
- Zookeeper
- Clojure

Kafka and Zookeper are included in the [Confluent platform 3.3.0](https://www.confluent.io/download/.)

## How it works

This application starts a long running Kafka streams task that will constantly do the following:

* Read play events from the input topic
* Group the tracks into session windows of 20 minutes
* Find the top 50 sessions by track counts
* Find the top 10 tracks from the top 50 sessions
* Output the top tracks into a output topic called `top-ten-track-counts-app-1`.

One thing to note is that parallelism is determined by the number of partitions in the input topic. If we have 10 partitions, then we're able to specify 10 threads when running the task from the command line. Alternatively we could also run 10 instances of the application.

## Usage

- Start Zookeeper and the Kafka broker.

- Create a Kafka topic to produce the play events:

```
kafka-topics --create --topic play-events --zookeeper localhost:2181 --replication-factor 1 --partitions 10
```

- Publish the play events from the `userid-timestamp-artid-artname-traid-traname.tsv` file into the topic:

```
lein run -m kafka-streams-lastfm.core/produce-play-events play-events userid-timestamp-artid-artname-traid-traname.tsv
```

- Check the events have been generated correctly:

```
kafka-console-consumer --zookeeper localhost:2181 --topic play-events --from-beginning
```

- Start the application to collect the top track counts:

```
lein run -m kafka-streams-lastfm.core/run app-1 play-events 10
```

- Check the results from the Kafka topic in a different shell:
```
kafka-console-consumer --from-beginning --zookeeper localhost:2181 --topic top-ten-tracks-app-1
```

## License

Copyright © 2017 Oliver Martell

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
