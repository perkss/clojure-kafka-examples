(ns kafka-example.core
  (:import  (java.util Properties)
            (org.apache.kafka.clients.consumer ConsumerConfig KafkaConsumer)
            (org.apache.kafka.common.serialization StringSerializer StringDeserializer)
            (org.apache.kafka.clients.producer KafkaProducer ProducerRecord))
  (:gen-class))

(defn -main
  [& args]

  (.addShutdownHook (Runtime/getRuntime) (Thread. #(println "Shutting down")))

  (def topic "example-topic")
  (def producer-topic "example-produced-topic")
  (def bootstrap-server "localhost:9092")

  (def consumer-props
    {"bootstrap.servers", bootstrap-server
     "group.id",          "My-Group"
     "key.deserializer",  StringDeserializer
     "value.deserializer", StringDeserializer
     "auto.offset.reset", "earliest"
     "enable.auto.commit", "true"})

  ;; Create a new kafka consumer with props
  (def consumer (doto (KafkaConsumer. consumer-props)
                  (.subscribe [topic])))

  (def producer-props {"value.serializer" StringSerializer
                       "key.serializer" StringSerializer
                       "bootstrap.servers" bootstrap-server})

  (def producer (KafkaProducer. producer-props))

  (println "Starting my kafka example app. With topic consuming topic" topic
           "and sending to topic" producer-topic)
   (while true

     (let [records (.poll consumer 10)]
      (doseq [record records]
        (println "Sending on value" (str "Value: " (.value record)))
        (.send producer (ProducerRecord. producer-topic (str "Value: " (.value record))))))

    (.commitAsync consumer)))
