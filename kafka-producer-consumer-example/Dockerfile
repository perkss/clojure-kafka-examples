FROM openjdk:8-jre-alpine
# COPY the build jar from the target folder
COPY target/uberjar/kafka-example*-standalone.jar kafka-example.jar
CMD ["/usr/bin/java", "-jar", "/kafka-example.jar"]