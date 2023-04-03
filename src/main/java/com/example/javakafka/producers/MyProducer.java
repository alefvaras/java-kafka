package com.example.javakafka.producers;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MyProducer {


    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "1");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("linger.ms","10");
        props.put("batch.size","32384");
        props.put("buffer.memory","33554432");
        try (Producer<String, String> producer = new
                KafkaProducer<>(props);) {
            for (int i = 0; i < 100; i++) {
                producer.send(new
//                        ProducerRecord<>("ale-topic3", (i%2==0)?"key-par":"key-impar", String.valueOf(i)));
                        ProducerRecord<>("ale-topic3", "key", String.valueOf(i)));
            }
            producer.flush();

        }

    }

}
