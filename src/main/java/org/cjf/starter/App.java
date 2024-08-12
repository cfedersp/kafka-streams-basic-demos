package org.cjf.starter;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

import java.io.FileInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.Arrays;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;

/**
 * Hello world!
 *
 */
@Slf4j
public class App 
{

        
        static void runKafkaStreams(final KafkaStreams streams) {
            final CountDownLatch latch = new CountDownLatch(1);
            streams.setStateListener((newState, oldState) -> {
                if (oldState == KafkaStreams.State.RUNNING && newState != KafkaStreams.State.RUNNING) {
                    latch.countDown();
                }
            });

            streams.start();

            try {
                latch.await();
            } catch (final InterruptedException e) {
                throw new RuntimeException(e);
            }

            log.info("Streams Closed");
        }
        static Topology buildToUpperTopology(String inputTopic, String outputTopic) {
            Serde<String> stringSerde = Serdes.String();
            StreamsBuilder builder = new StreamsBuilder();

                builder
                .stream(inputTopic, Consumed.with(stringSerde, stringSerde))
                .peek((k,v) -> log.info("Observed event: {}", v))
                .mapValues(s -> s.toUpperCase())
                .peek((k,v) -> log.info("Transformed event: {}", v))
                .to(outputTopic, Produced.with(stringSerde, stringSerde));


            return builder.build();
        }
    static Topology buildFlatMapTopology(String inputTopic, String outputTopic) {
        Serde<String> stringSerde = Serdes.String();
        StreamsBuilder builder = new StreamsBuilder();

        builder
            .stream(inputTopic, Consumed.with(stringSerde, stringSerde))
            .peek((k,v) -> log.info("Observed event: {}", v))
            .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
            .peek((k,v) -> log.info("flattened word  event: {}", v))
            .to("WordCs", Produced.with(stringSerde, stringSerde));

        return builder.build();
    }
    static Topology buildGroupByAndCountTopology(String inputTopic, String outputTopic) {
        Serde<String> stringSerde = Serdes.String();
        StreamsBuilder builder = new StreamsBuilder();

        builder
            .stream(inputTopic, Consumed.with(stringSerde, stringSerde))
            .peek((k,v) -> log.info("Observed event: {}", v))
            .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
                .groupBy((key, word) -> word)
            //.count(Materialized.<String, Long, KeyValueStore<Bytes, Long>>as("counts-store").withValueSerde(Serdes.Long()))
            //.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store").withValueSerde(Serdes.Long()));
             //   .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"));
        .count()
        .toStream()
            //.peek((k,v) -> log.info("word count event: {}", v))
            .to("WordCounts", Produced.with(stringSerde, Serdes.Long()));

        return builder.build();
    }
    static Topology buildGroupByKeyAndCountTopology(String inputTopic, String outputTopic) {
        Serde<String> stringSerde = Serdes.String();
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source =  builder.stream(inputTopic, Consumed.with(stringSerde, stringSerde));
        source.peek((k,v) -> log.info("Observed event: {}", v))
            .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
            .map((key, value) -> new KeyValue<>(value, value))
            .groupByKey()
            //.count(Named.as("counts-store"))
            .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store").with(stringSerde, Serdes.Long()))
            .mapValues(value->Long.toString(value))
            .toStream(Named.as("string-counts")) //"WordCounts", (k, v) -> new KeyValue<>(k, v));
            .peek((k,v) -> log.info("word count event: {}:{}", k, v))
            .to(outputTopic, Produced.with(stringSerde, stringSerde));

        return builder.build();
    }
    static Topology buildOssCountTopology(String inputTopic, String outputTopic) {
            // default.key.serde=org.apache.kafka.common.serialization.Serdes$StringSerde
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // work-around for an issue around timing of creating internal topics
        // Fixed in Kafka 0.10.2.0
        // don't use in large production apps - this increases network load
        // props.put(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, 500);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source = builder.stream(inputTopic);


        final Pattern pattern = Pattern.compile("\\W+");
        source.flatMapValues(value-> Arrays.asList(pattern.split(value.toLowerCase())))
            .peek((k,v) -> log.info("Observed event: {}", v))
            .map((key, value) -> new KeyValue<Object, Object>(value, value))
            .filter((key, value) -> (!value.equals("the")))
            .groupByKey()
            .count()
            //.mapValues(value->Long.toString(value))
            .toStream()
            .to(outputTopic); // , Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }
    static Topology buildTimeWindowedCountTopology(String inputTopic, String outputTopic) {
            //default.key.serde=org.apache.kafka.streams.kstream.WindowedSerdes$TimeWindowedSerde
        // default.key.serde=org.apache.kafka.streams.kstream.WindowedSerdes$TimeWindowedSerde
        //windowed.inner.class.serde=org.apache.kafka.common.serialization.Serdes$StringSerde
        //default.value.serde=org.apache.kafka.common.serialization.Serdes$StringSerde
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // work-around for an issue around timing of creating internal topics
        // Fixed in Kafka 0.10.2.0
        // don't use in large production apps - this increases network load
        // props.put(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, 500);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source = builder.stream(inputTopic);


        final Pattern pattern = Pattern.compile("\\W+");
        source.flatMapValues(value-> Arrays.asList(pattern.split(value.toLowerCase())))
            .peek((k,v) -> log.info("Observed event: {}", v))
            //.map((key, value) -> new KeyValue<Object, Object>(value, value))
            //.filter((key, value) -> (!value.equals("the")))
            //.groupByKey()
            .groupBy((key, value) -> value, Grouped.with(Serdes.String(), Serdes.String()))
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1L)))
            //.count()
            //             .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("count2").with(Serdes.String(), Serdes.Long())).suppress(untilWindowCloses(unbounded()))
            //.count(Materialized.with(Serdes.Bytes(), Serdes.ByteArray()).<String, Long, WindowStore<Bytes, byte[]>>as("count2")).suppress(untilWindowCloses(unbounded()))
            .count().suppress(untilWindowCloses(unbounded()))
            //.mapValues(value->Long.toString(value))
            .toStream()
            .map((key, value) -> new KeyValue<String, Long>(key.key() + "@" + key.window().start() + "->" + key.window().end(), value))

            //counts.map((wk, value) -> KeyValue.pair(wk.key, value))
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long())); //, (Produced<String, Long>) Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class,1), Serdes.Long()));

        return builder.build();
    }
        public static void main(String[] args) throws Exception {

            if (args.length < 1) {
                throw new IllegalArgumentException("This program takes one argument: the path to a configuration file.");
            }

            Properties props = new Properties();
            try (InputStream inputStream = new FileInputStream(args[0])) {
                props.load(inputStream);
            }

            final String inputTopic = props.getProperty("input.topic.name");
            final String outputTopic = props.getProperty("output.topic.name");

            try (DataUtil utility = new DataUtil()) {

                utility.createTopics(
                    props,
                    Arrays.asList(
                        new NewTopic(inputTopic, Optional.empty(), Optional.empty()),
                        new NewTopic(outputTopic, Optional.empty(), Optional.empty())));

                // Ramdomizer only used to produce sample data for this application, not typical usage
                try (DataUtil.Randomizer rando = utility.startNewRandomizer(props, inputTopic, 3)) {

                    KafkaStreams kafkaStreams = new KafkaStreams(
                        buildOssCountTopology(inputTopic, outputTopic), props);

                    Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

                    log.info("Kafka Streams 101 App Started");
                    runKafkaStreams(kafkaStreams);

                }
            }
        }
}
