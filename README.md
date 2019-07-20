## About

A selection of examples using Kafka and Kafka Streams with Clojure. I wanted to learn the real Kafka API via Clojure not via a Clojure wrapper so here are examples of using the raw API, which is clean and means you do not need to wait for Clojure wrapper libraries to upgrade Kafka. 

Blog post here: https://perkss.github.io/#/clojure/KafkaClojure#text-body

## Prerequisites
Expected to be able to set up Zookeeper and Kafka Broker to run the examples. Or use the Confluent platform. 
The Kafka Stream Examples all have relevant tests rather than running a Jar. Please check individual module README to check how to run.

## Unit Test
Check out the unit tests for each file to see it in action without the need for running with Zookeeper and Kafka. (Intention to add in Docker file to make it easier)

## Examples
* Kafka Producer and Consumer in Clojure using Java interop.
* Kafka Streaming example to upper case of strings.
* Kafka Streaming testing example.
* Kafka Streaming join with two Streams (Left Join, Inner Join, Outer Join)
* Kafka Streaming join with KTABLE.
* Avro Kafka deserialization and serialization.
* More examples to come regularly watch this space ...

## Contributions

* Please get involved and get recognition. If you want to add more examples then raise a PR. Or raise issues for ideas on examples that would be beneficial.  
