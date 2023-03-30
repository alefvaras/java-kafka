package com.example.javakafka.multithread;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

public class ThreadConsumer extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThreadConsumer.class);

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final KafkaConsumer<String, String> consumer;

    public ThreadConsumer(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(Arrays.asList("ale-topic3"));

            while (!closed.get()) {

                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(10000));
                for (ConsumerRecord<String, String> record
                        : records)
                    LOGGER.info(String.format("offset =  %s, partition =  %s , key =  %s, value=  %s",
                            record.offset(), record.partition(),record.key(), record.value()));
            }
        } catch (WakeupException e) {
            if (!closed.get()) {
                throw e;

            }
        } finally {
            consumer.close();
            shutdown();
        }
    }


    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }
}
