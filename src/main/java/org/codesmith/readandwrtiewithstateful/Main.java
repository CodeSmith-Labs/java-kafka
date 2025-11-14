package org.codesmith.readandwrtiewithstateful;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Author: subramanyamadimulam
 * Date: 13/11/25
 */

/**
 * Before executing create kafka topics with defined or custom names
 * Commands to test :
 * kafka-console-producer --topic streams-text-input --bootstrap-server localhost:9092 --> To publish to topic
 * kafka-console-consumer --topic stream-write-topic --bootstrap-server localhost:9092 --from-beginning --property print.key=true --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer --> To output from topic
 */

public class Main {

    public static void main(String[] args) throws IOException {
        //Create configuration
        final Properties props = KafkaStreamsConfig.streamsConfig();

        //Create Topology (computation logic) using builder
        final StreamsBuilder builder = new StreamsBuilder();

        // Mention topics name of source and destination
        KStream<String, String> stream = builder.stream("streams-read");
        stream.flatMapValues(value -> Arrays.asList(value.split("\\W+"))).groupBy((key, value) -> value.toLowerCase())
                .count().toStream().to("streams-write", Produced.with(Serdes.String(), Serdes.Long()));

        final Topology topology = builder.build();
        System.out.println(topology.describe());

        //Using topology build client
        final KafkaStreams streams = new KafkaStreams(topology, props);

        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }
}
