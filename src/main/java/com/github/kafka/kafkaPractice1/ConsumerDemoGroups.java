package com.github.kafka.kafkaPractice1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoGroups {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class.getName());
        String groupId = "MY-fifth-application";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");//earliest-from beginning, latest-only new msg,none- manual offset
        //create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);
        //subscribe consumer to our topic
        consumer.subscribe(Collections.singleton("second_topic"));//Array.asList() for multiple topics
        //poll for new data
while(true){
    ConsumerRecords<String,String> records= consumer.poll(Duration.ofMillis(100));
    for (ConsumerRecord<String, String> record :records) {
       logger.info("Key: "+ record.key()+"Value: "+record.value());
       logger.info("Partition:"+record.partition()+"Offset: "+record.offset());

    }
}
    }
}
