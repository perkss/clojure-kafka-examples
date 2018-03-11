
# Start Zookeeper
/usr/local/zookeeper/bin/zkServer.sh start
# Start Kafka
/usr/local/kafka/bin/kafka-server-start.sh -daemon /usr/local/kafka/config/server.properties
# Create topic topic that the application consums from 
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic example-topic
# Create the consuming topic from the example that produces to it
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic example-produced-topic
# Produce messages for the app to consumer
/usr/local/kafka/bin/kafka-console-producer.shroker-list localhost:9092 --topic example-topic
# Consume the output messages
/usr/local/kafka/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic example-produced-topic --from-beginning
