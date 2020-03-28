#!/bin/sh
echo "Running Kafka Producer Consumer. You must have confluent platform on your
 path installed from: https://docs.confluent.io/current/quickstart/index.html."

### Note: WE ADVISE USE DOCKER-COMPOSE. Use this if you want to use the confluent install

export BOOTSTRAP_SERVER=localhost:9092
export ZOOKEEPER_HOSTS=localhost:2181

#Expects confluent platform to be on the path
confluent start

# Run the app straight with lein
#(cd ../
# lein run
#)

# Make the uberjar
(cd ../
  lein uberjar
)

# Run the jar to start the app
(cd ../
 java -jar target/uberjar/kafka-example-0.1.0-SNAPSHOT-standalone.jar
)




# Path needs to match for Kafka Console Producer run this and produce data
#/usr/local/bin/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic example-consumer-topic
