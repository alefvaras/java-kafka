package com.example.javakafka.callbacks;

import com.example.javakafka.consumers.MyConsumer;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class CallbackProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(CallbackProducer.class);

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
//                producer.send(new
//                        ProducerRecord<>("ale-topic3", "key " + i, "message"), new Callback() {
//                    @Override
//                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//                        if(e !=null) {
//                            LOGGER.error(e.getMessage());
//                        }else{
//                            LOGGER.info("topic={} offset={}",recordMetadata.topic(),recordMetadata.offset());
//                        }
//                    }
//                });

                producer.send(new ProducerRecord<>("ale-topic3", "key " + i, "message"),(recordMetadata,e)->{
                        if(e !=null) {
                            LOGGER.error(e.getMessage());
                        }else{
                            LOGGER.info("topic={} offset={}",recordMetadata.topic(),recordMetadata.offset());
                        }
                    }
                );
            }
            producer.flush();

        }

    }
}
