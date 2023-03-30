package com.example.javakafka.multithread;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MainThreadConsumer {


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

        ExecutorService executorService= Executors.newFixedThreadPool(5);


        for (int i = 0; i <5;i++) {
            ThreadConsumer consumer=new ThreadConsumer(new KafkaConsumer<>(props));
            executorService.execute(consumer);


        }
        executorService.shutdown();
        while (!executorService.isTerminated());
    }
}
