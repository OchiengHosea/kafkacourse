package org.duke.learning;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    // create producer properties
    private final Properties properties = new Properties();
    private final KafkaProducer<String,String> producer;
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public ProducerDemo(){
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(getProperties());
    }

    public Properties getProperties() {
        return properties;
    }

    //create producer
    public KafkaProducer<String, String> getProducer() {
        return producer;
    }

    public ProducerRecord<String,String> createRecord(String topic, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, value);
        return record;
    }

    //send data
    public void sendDemoData() {
        getProducer().send(createRecord("first_topic", "hello from code"));
        getProducer().flush();
        getProducer().close();
    }
}
