package org.codesmith.wordcountdemo;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Author: subramanyamadimulam
 * Date: 11/11/25
 */

public class Main {

    public static void main(String[] args) throws IOException {
        final Properties props = KafkaStreamsConfig.streamsConfig(args);

        final StreamsBuilder builder = new StreamsBuilder();
        KafkaWordCountStream.createWordCountStream(builder);
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
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
