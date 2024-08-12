package org.cjf.starter;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Slf4j
public class DataUtil implements AutoCloseable {
    private ExecutorService executorService = Executors.newFixedThreadPool(1);

    public class Randomizer implements AutoCloseable, Runnable {
        private Properties props;
        private String topic;
        private Producer<String, String> producer;
        private boolean closed;
        private int maxMsgs;

        public Randomizer(Properties producerProps, String topic, int maxMsgs) {
            this.closed = false;
            this.topic = topic;
            this.props = producerProps;
            this.props.setProperty("client.id", "faker");
            this.maxMsgs = maxMsgs;
        }

        public void run() {
            try (KafkaProducer producer = new KafkaProducer<String, String>(props)) {
                Faker faker = new Faker();
                int msgCount = 0;
                while (!closed && maxMsgs > 0 && msgCount <= maxMsgs) {
                    try {
                        String newFact = faker.chuckNorris().fact();
                        Object result = producer.send(new ProducerRecord<>(
                            this.topic,
                            newFact)).get();
                        log.info("Sent fact to topic {} {}", this.topic, newFact);
                        ++msgCount;
                        Thread.sleep(30000);
                    } catch (InterruptedException e) {
                    }
                }
            } catch (Exception ex) {
                log.error(ex.toString());
            }
        }
        public void close()  {
            closed = true;
        }
    }

    public Randomizer startNewRandomizer(Properties producerProps, String topic, int maxMsgs) {
        Randomizer rv = new Randomizer(producerProps, topic, maxMsgs);
        executorService.submit(rv);
        return rv;
    }

    public void createTopics(final Properties allProps, List<NewTopic> topics)
        throws InterruptedException, ExecutionException, TimeoutException {
        try (final AdminClient client = AdminClient.create(allProps)) {
            log.info("Creating topics");

            client.createTopics(topics).values().forEach( (topic, future) -> {
                try {
                    future.get();
                } catch (Exception ex) {
                    log.info(ex.toString());
                }
            });

            Collection<String> topicNames = topics
                .stream()
                .map(t -> t.name())
                .collect(Collectors.toCollection(LinkedList::new));

            log.info("Asking cluster for topic descriptions");

            client
                .describeTopics(topicNames)
                .allTopicNames()
                .get(10, TimeUnit.SECONDS)
                .forEach((name, description) -> log.info("Topic Description: {}", description.toString()));


        }
    }

    public void close() {
        if (executorService != null) {
            executorService.shutdownNow();
            executorService = null;
        }
    }
}
