# kafka-example

An example showing how to use Kafka producer and consumer using Clojure and the Java API via Java interop. Simple example that consumers from a topic logs the input and then sends on the value to another topic for the user to see. You can run it as a plain jar with all the Kafka Broker and Zookeeper running or you can use the Docker set up we have done and follow the blog post https://perkss.github.io/#/DevOps! Up to you.

## Installation

Requires Zookeeper and Kafka to be set up. Check the ports match from the code.
Checkout the project view the start script and follow those commands.

You can also just install confluent platform and run the start-example.sh script from the bin directory.

## Usage

In the project directory run :

    $ lein uberjar

    $ java -jar target/uberjar/kafka-example-0.1.0-SNAPSHOT-standalone.jar

This starts the project and you should see it log out that it has started.

You then need to set up the Kafka topics, producer and consumer as specified in the start.sh script. Once set up you can produce to the example topic for example Hello

With the app running it will log out:

    INFO  kafka-example.core: Sending on value Value: Hello

Then with a consumer on the example-produced-topic it will log out Value: Hello

## Running with Docker and Confluent

Docker makes the above very simple.

To start the required Kafka and Zookeeper run:
    
    $ docker-compose up -d
    
To build the example docker image and run it follow:
    
    $ docker build -t producer-consumer-example .

    $ docker run -i -t producer-consumer-example
  
Then we need to create the required topics not we reference the docker-compose names for kafka-broker and zookeeper. Rather than if running on bare metal the local host.

    $ docker-compose exec kafka-broker kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic example-topic
    $ docker-compose exec kafka-broker kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic example-produced-topic
    
Now lets check they have been created by listing them

    $ docker-compose exec kafka-broker kafka-topics --zookeeper zookeeper:2181 --list
    
Now create the consumer to listen to messages you will eventually produce and will be processed by Java.

    $ docker-compose exec kafka-broker kafka-console-consumer --bootstrap-server kafka-broker:9092 --topic example-topic
    
Now create the producer to send messages that will be processed

    $ docker-compose exec kafka-broker kafka-console-consumer --bootstrap-server localhost:9092 --topic example-produced-topic --from-beginning
    
    
# Produce messages for the app to consumer
    

## Example

Have fun with the example, kept very simple purposely to show the Java interop API of Kafka Clients in Clojure.
