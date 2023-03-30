package com.example.javakafka.transactional;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class TProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("transactional.id", "ale_trx_producer");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("linger.ms","10");
        props.put("batch.size","32384");
        props.put("buffer.memory","33554432");
        try (Producer<String, String> producer = new
                KafkaProducer<>(props);) {
            producer.initTransactions();
            producer.beginTransaction();
            for (int i = 0; i < 100; i++) {
                producer.send(new
                        ProducerRecord<>("ale-topic3", "key "+i, "message"));
            }
            producer.commitTransaction();
            producer.flush();

        }

    }
}
