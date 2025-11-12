package org.codesmith.wordcountdemo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

/**
 * Author: subramanyamadimulam
 * Date: 11/11/25
 */

public class KafkaStreamsConfig {

    private static final String CONFIG_PROPERTIES_FILE_PATH = "config.properties";

    public static Properties streamsConfig() throws IOException {
        final Properties props = new Properties();
        ClassLoader classLoader = KafkaStreamsConfig.class.getClassLoader();
        try (final FileInputStream fis = new FileInputStream(Objects.requireNonNull(classLoader.getResource(CONFIG_PROPERTIES_FILE_PATH)).getFile())) {
            props.load(fis);
        }
//        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
//        props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        props.putIfAbsent(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
//        props.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
//        props.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
//        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }
}
