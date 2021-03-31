package com.github.kafka.kafkaPractice1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());
        String topic = "second_topic";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");//earliest-from beginning, latest-only new msg,none- manual offset
        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
//Assign and seek are mostly used to replay data or fetch a specific message
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        long offset = 15l;
        consumer.assign(Arrays.asList(topicPartition));
        consumer.seek(topicPartition, offset);

        boolean keepReading = true;
        int numberOfMessageReadSoFar = 0;
        int numberOfMessageToRead = 0;
        //poll for new data
        while (keepReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                numberOfMessageReadSoFar += 1;
                logger.info("Key: " + record.key() + "Value: " + record.value());
                logger.info("Partition:" + record.partition() + "Offset: " + record.offset());
                if (numberOfMessageReadSoFar >= numberOfMessageToRead) {
                    keepReading = false;
                    break;
                }
            }
        }
        logger.info("Exiting the application");
    }
}
