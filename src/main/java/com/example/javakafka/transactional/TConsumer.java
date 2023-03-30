package com.example.javakafka.transactional;

import com.example.javakafka.consumers.MyConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class TConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(TConsumer.class);

    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "ale-group");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("isolation.level","read_committed");
        props.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        try (KafkaConsumer<String, String> consumer = new
                KafkaConsumer<>(props);) {
            consumer.subscribe(Arrays.asList("ale-topic3"));

            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record
                        : records)
                    LOGGER.info(String.format("offset = %d, partition = %d , key = %d, value= %d ",
                            record.offset(), record.partition(),record.key(), record.value()));
            }
        }

    }
}
