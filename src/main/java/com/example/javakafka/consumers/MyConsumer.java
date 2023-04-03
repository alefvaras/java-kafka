package com.example.javakafka.consumers;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class MyConsumer {


   private static final Logger LOGGER = LoggerFactory.getLogger(MyConsumer.class);


    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "ale-group");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");

        try (KafkaConsumer<String, String> consumer = new
                KafkaConsumer<>(props);) {
//            consumer.subscribe(Arrays.asList("ale-topic3"));
            TopicPartition topicPartition=new TopicPartition("ale-topic3",1);

            consumer.assign(Arrays.asList(topicPartition));
            consumer.seek(topicPartition,47);
            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record
                        : records)
                    LOGGER.info(String.format("offset =  %s, partition =  %s , key =  %s, value=  %s",
                            record.offset(), record.partition(),record.key(), record.value()));
                consumer.commitSync();
            }
        }

            }
        }


