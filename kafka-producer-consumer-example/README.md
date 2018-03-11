# kafka-example

An example showing how to use Kafka producer and consumer using Clojure and the Java API via Java interop. Simple example that consumers from a topic logs the input and then sends on the value to another topic for the user to see. 

## Installation

Requires Zookeeper and Kafka to be set up. Check the ports match from the code.
Checkout the project view the start script and follow those commands.

## Usage

In the project directory run : 
    $ lein uberjar

Then :
    $ java -jar kafka-example-0.1.0-standalone.jar

This starts the project and you should see it log out that it has started.

## Example

Have fun with the example, kept very simple purposely to show the Java interop API of Kafka Clients in Clojure. 

