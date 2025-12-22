package org.example;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

public class KafkaExampleProducer {

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19094");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "client-" + UUID.randomUUID());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        int MSG_COUNT = 3;
        String topic = "delay_topic";
        long defaultDelayInSeconds = 60*1000L;
        Gson gson = new Gson();
        for (int i=0; i< MSG_COUNT; i++) {
            DelayMessage dm = new DelayMessage();
            dm.setMsg(String.valueOf(i));
            dm.setDeliverTs(System.currentTimeMillis() + defaultDelayInSeconds);

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, gson.toJson(dm));
            producer.send(record);
        }
        Thread.sleep(500);
    }
}
