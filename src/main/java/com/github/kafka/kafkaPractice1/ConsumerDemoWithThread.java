 package com.github.kafka.kafkaPractice1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    public ConsumerDemoWithThread() {
    }

    public void run() {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());
        String topic = "second_topic";
        String groupId = "MY-sixth-application";
        String bootStrapServers = "127.0.0.1:9092";
        CountDownLatch latch = new CountDownLatch(1);
        logger.info("creating consumer thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(latch, bootStrapServers, groupId, topic);
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();
        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Cought shutdown hook");
            ((ConsumerRunnable)myConsumerRunnable).shutdown();
            try {
                latch.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has existed");
        }));
        try {
            latch.wait();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted:",e);
        }finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {
        private CountDownLatch latch;
        Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
        KafkaConsumer<String, String> consumer;
        //String topic = "second_topic";
       // String groupId = "MY-sixth-application";
        //String bootStrapServers = "127.0.0.1:9092";

        public ConsumerRunnable(CountDownLatch latch, String bootStrapServers, String groupId, String topic) {
            this.latch = latch;
          //  this.topic = topic;
            //this.bootStrapServers = bootStrapServers;
            //this.groupId = groupId;
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");//earliest-from beginning, latest-only new msg,none- manual offset

            //create consumer
            consumer = new KafkaConsumer<>(properties);
            //subscribe consumer to our topic
            consumer.subscribe(Arrays.asList("second_topic"));//Array.asList() for multiple topics
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + "Value: " + record.value());
                        logger.info("Partition:" + record.partition() + "Offset: " + record.offset());

                    }
                }
            } catch (WakeupException e) {
                logger.info("Received Shutdown signal!");
            } finally {
                consumer.close();
                latch.countDown();
            }

        }

        public void shutdown() {
            //the wakeup() method is a special method to interrupt consumer.poll()
            //it will throw the exception WakeUpException
            consumer.wakeup();
        }
    }
}
