# Setup the env
Start Zookeeper
cd $KAFKA_HOME
./bin/zookeeper-server-start.sh config/zookeeper.properties
Start Kafka
cd $KAFKA_HOME
./bin/kafka-server-start.sh config/server.properties
Start Kafka CLI Consumer
$KAFKA_HOME/bin/kafka-console-consumer.sh --topic tall-random-groups14 --from-beginning --bootstrap-server localhost:9092  --property print.key=true --value-deserializer "org.apache.kafka.common.serialization.LongDeserializer"

# Build and Run:
mvn package
java -jar target/kafka-streams-demo-1.0-SNAPSHOT.jar src/main/resources/config/dev.properties

# Clean up
rm -rf /tmp/kafka-logs/
rm -rf /tmp/zookeeper/