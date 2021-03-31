package com.github.kafka.kafkaPractice1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerKeyDemo {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerKeyDemo.class);
        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //create producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
//create record
        for (int i = 10; i < 20; i++) {
            String topic = "second_topic";
            String value = "hello first message" + Integer.toString(i);
            String key = "id_"+ Integer.toString(i);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic,key,value );

            //send data
            kafkaProducer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //execute every time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        //The record was successful sent
                        logger.info("Received new metadata, \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());

                    } else {
                        logger.error("Error while producing" + e);
                    }
                }
            });
        }
//flush data
            kafkaProducer.flush();
            //close producer
            kafkaProducer.close();
        }

}
