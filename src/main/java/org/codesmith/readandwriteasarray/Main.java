package org.codesmith.readandwriteasarray;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;

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
        builder.stream("streams-read").flatMapValues(value -> Arrays.asList(((String)value).split(" "))).to("streams-write");

        //Using topology build client
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);

        CountDownLatch latch = new CountDownLatch(1);

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
