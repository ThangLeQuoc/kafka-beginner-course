package com.github.thanglequoc.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {
        // Create producer properties
        Properties properties = new Properties();

        String originalBootstrapServerAddr = "127.0.0.1:9092";
        String altWSLBootstrapServerAddr = "[::1]:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, altWSLBootstrapServerAddr);
        // see issue on https://stackoverflow.com/a/66379094/5668956

        // this serializer help the producer know what kind of data being send to kafka
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Create producer record
        ProducerRecord<String, String> record = new ProducerRecord<>("second_topic", "Hello world");

        producer.send(record);

        // flush the data
        // producer.flush();
        producer.close();


        System.out.println("Hello World");
    }
}
