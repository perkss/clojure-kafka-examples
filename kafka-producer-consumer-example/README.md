# kafka-example

An example showing how to use Kafka producer and consumer using Clojure and the Java API via Java interop. Simple example that consumers from a topic logs the input and then sends on the value to another topic for the user to see. You can run it as a plain jar with all the Kafka Broker and Zookeeper running or you can use the Docker set up we have done and follow the blog post https://perkss.github.io/#/DevOps! Up to you.

## Integration Test
To get started simply have Docker running and run `lein test` and this will run the integration test where Kafka is started up 
we start our application up, drop a test message on the topic and consume if after being processed by our application on the 
output topic. 

## Installation

Requires Zookeeper and Kafka to be set up. Please check the main README to run the bundled `docker-compose` file. Check the ports match from the code.
Checkout the project view the start script and follow those commands.

Alternatively you can also just install confluent platform docker version and start it. Or install Kafka locally and set the location the same as `/usr/local/bin/kafka/bin/` and `/usr/local/zookeeper/bin/` and then run the start.sh script from the bin directory.

## Usage

In the project directory run :

    $ lein uberjar

    $ java -jar target/uberjar/kafka-example-0.1.0-SNAPSHOT-standalone.jar

This starts the project and you should see it log out that it has started.

You then need to set up the Kafka topics, producer and consumer as specified in the start.sh script. Once set up you can produce to the example topic for example Hello

With the app running it will log out:

    INFO  kafka-example.core: Sending on value Value: Hello

Then with a consumer on the example-produced-topic it will log out Value: Hello

## Running with Docker

Docker makes the above very simple.

To start the required Kafka and Zookeeper run the following in the parent directory this starts a separate docker network with these two exposing them on localhost:
    
    $ ../docker-compose up -d
    
To build the example docker image and run it follow:
    
    $ docker build -t producer-consumer-example .

Please note we use localhost in the boostrap server and the zookeeper host so we are required to pass --network="host" to make this container point to the localhost of our machine where we have exposed our Kafka and Zookeeper instances.

    $ docker run --network="host" -i -t producer-consumer-example
  
Then we need to create the required topics note we reference the docker-compose names for broker and zookeeper. Rather than if running on bare metal the localhost.

    $ docker exec broker kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic example-topic
    $ docker exec broker kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic example-produced-topic
    
Now lets check they have been created by listing them if running from the docker-compose file location

    $ docker exec broker kafka-topics --zookeeper zookeeper:2181 --list
    
Or pass localhost and from the machine

    $ /usr/local/bin/kafka/bin/kafka-topics.sh --list --zookeeper localhost:2181
    
Now create the consumer to listen to messages you will eventually produce and will be processed by Java.

    $ docker exec broker kafka-console-consumer --bootstrap-server broker:9092 --topic example-produced-topic --from-beginning

Or from the local machine instance

    $ /usr/local/bin/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic example-produced-topic --from-beginning
    
Now create the producer to send messages that will be processed

    $ docker exec broker -it kafka-console-producer --broker-list broker:9092 --topic example-consumer-topic
    
Or from the local

    $ /usr/local/bin/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic example-consumer-topic
        

## Example

Have fun with the example, kept very simple purposely to show the Java interop API of Kafka Clients in Clojure.


## Security TLS

A great tutorial on TLS and Kafka is available from [Confluent](https://docs.confluent.io/current/security/security_tutorial.html#generating-keys-certs).


