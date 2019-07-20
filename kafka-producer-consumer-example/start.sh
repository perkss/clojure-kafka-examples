
# Start Zookeeper
/usr/local/zookeeper/bin/zkServer.sh start
# Start Kafka
/usr/local/bin/kafka/bin/kafka-server-start.sh -daemon /usr/local/kafka/config/server.properties
# Create topic topic that the application consumes from
/usr/local/bin/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic example-consumer-topic
# Create the consuming topic from the example that produces to it
/usr/local/bin/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic example-produced-topic
# List the created
/usr/local/bin/kafka/bin/kafka-topics.sh --list --zookeeper localhost:2181
# Produce messages for the app to consumer
/usr/local/bin/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic example-consumer-topic
# Consume the output messages
/usr/local/bin/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic example-produced-topic --from-beginning
