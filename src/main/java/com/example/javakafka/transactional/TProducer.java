package com.example.javakafka.transactional;

import com.example.javakafka.consumers.MyConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class TProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(TProducer.class);

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

            try {
                producer.initTransactions();
                producer.beginTransaction();
                for (int i = 0; i < 100; i++) {
                    producer.send(new
                            ProducerRecord<>("ale-topic3", "key "+i, "message"));

                    if(i==50) throw new Exception("Invalid");
                }


                producer.commitTransaction();
                producer.flush();

            }catch (Exception e) {
                LOGGER.error(e.getMessage());
                producer.abortTransaction();
            }


        }

    }
}
