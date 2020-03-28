## About

A selection of examples using Kafka and Kafka Streams with Clojure. I wanted to learn the real Kafka API via Clojure not via a Clojure wrapper so here are examples of using the raw API, which is clean and means you do not need to wait for Clojure wrapper libraries to upgrade Kafka. 

Blog post here: https://perkss.github.io/#/clojure/KafkaClojure#text-body

## Prerequisites
Expected to be able to set up Zookeeper and Kafka Broker to run the examples. Or use the Confluent platform. 
The Kafka Stream Examples all have relevant tests rather than running a Jar. Please check individual module README to check how to run.

## Unit Test
Check out the unit tests for each file to see it in action without the need for running with Zookeeper and Kafka.

## Examples
* Kafka Producer and Consumer in Clojure using Java interop.
* Search a Topic and return records that match a Key from beginning to current point in time.
* Kafka Streaming example to upper case of strings.
* Kafka Streaming testing example.
* Kafka Streaming join with two Streams (Left Join, Inner Join, Outer Join)
* Kafka Streaming join with KTABLE.
* Avro Kafka deserialization and serialization.
* KStreamGroup Aggregate 
* TopologyTestDriver
* More examples to come regularly watch this space ...

## Integration Testcontainers
Testcontainers provide the ability to test applications that have docker interactions simply by starting up your containers 
in our examples Kafka we can then start our applications up against this container and run the tests in a fully integrated 
test with Kafka. Awesome!

## Kafka and Zookeeper

We have provided our own docker-compose file that will start Kafka and Zookeeper via localhost, so other containers or local apps can access the broker.

Start this using the command `docker-compose up -d`

```yaml
version: '3'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:5.4.1
    hostname: zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
  broker:
    image: confluentinc/cp-enterprise-kafka:5.4.1
    hostname: broker
    container_name: broker
    depends_on:
      - zookeeper
    ports:
      - "29092:29092"
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
```

## Contributions

* Please get involved and get recognition. If you want to add more examples then raise a PR. Or raise issues for ideas on examples that would be beneficial.  
