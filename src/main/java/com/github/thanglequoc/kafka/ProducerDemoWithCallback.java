package com.github.thanglequoc.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {
        Logger LOGGER = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

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

        for (int i = 0; i < 20; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("second_topic", "Hello world " + Integer.toString(i));

            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // execute everytime a record is successfully or an exception is thrown
                    if (exception != null) {
                        LOGGER.error("Unable to send record", exception);
                    } else {
                        LOGGER.info("Received new metadata. \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp());
                    }
                }
            });
        }
        // flush the data
        // producer.flush();
        producer.close();


        System.out.println("Hello World");
    }
}
