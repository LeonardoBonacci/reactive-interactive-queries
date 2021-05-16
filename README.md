# react-interactive-queries

test123 

> java -jar target/react-interactive-query-service-0.0.1-SNAPSHOT.jar --server.port=8083 --spring.cloud.stream.kafka.streams.binder.configuration.application.server=localhost:8083
> java -jar target/interactive-query-service-0.0.1-SNAPSHOT.jar --server.port=8083 --spring.cloud.stream.kafka.streams.binder.configuration.application.server=localhost:8083
> ./kafka-console-producer --broker-list localhost:9092 --topic count-us
> ./kafka-console-consumer      --bootstrap-server localhost:9092      --topic count-us
> ./kafka-console-consumer      --bootstrap-server localhost:9092      --topic counted     --key-deserializer org.apache.kafka.common.serialization.StringDeserializer    --value-deserializer org.apache.kafka.common.serialization.LongDeserializer         --property print.key=true
