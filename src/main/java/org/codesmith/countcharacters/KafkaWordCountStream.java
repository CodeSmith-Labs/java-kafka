package org.codesmith.countcharacters;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Locale;

/**
 * Author: subramanyamadimulam
 * Date: 11/11/25
 */

class KafkaWordCountStream {

    public static final String INPUT_TOPIC = "streams-text-input";
    public static final String OUTPUT_TOPIC = "streams-charactercount-output";
    public static void createWordCountStream(final StreamsBuilder builder) {
        final KStream<String, String> source = builder.stream(INPUT_TOPIC);

        final KStream<String, Long> stringLongKStream = source.selectKey(((key, value) -> value)).mapValues(value -> (long)value.length());

        // need to override value serde to Long type
        stringLongKStream.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
    }
}
