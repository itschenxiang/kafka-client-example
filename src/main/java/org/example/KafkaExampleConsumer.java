package org.example;

import ch.qos.logback.classic.Level;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

@Slf4j
public class KafkaExampleConsumer {
    public static void main(String[] args) {
        ((ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger("org.apache.kafka")).setLevel(Level.INFO);

        String topic = "delay_topic";
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:19094");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", StringDeserializer.class);
        props.put("value.deserializer", StringDeserializer.class);
        props.put("group.id", "example-group");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        Gson gson = new Gson();
        Map<TopicPartition, Long> pausePartitionMap = new HashMap<>();
        while (true) {
            try {
                long curTs = System.currentTimeMillis();
                Map<TopicPartition, Long> tmpPausePartitionMap = new HashMap<>();
                for (TopicPartition pausePartition : pausePartitionMap.keySet()) {
                    if (pausePartitionMap.get(pausePartition) <= curTs) {
                        log.info("resume consume partitions {}", pausePartition);
                        consumer.resume(List.of(pausePartition));
                    } else {
                        tmpPausePartitionMap.put(pausePartition, pausePartitionMap.get(pausePartition));
                    }
                }
                pausePartitionMap = tmpPausePartitionMap;
                log.info("pause partitions set {}", pausePartitionMap.keySet());
                ConsumerRecords<String, String> records = consumer.poll(100L);
                if (!records.isEmpty()) {
                    // 按照分区处理
                    for (TopicPartition rp : records.partitions()) {
                        List<ConsumerRecord<String, String>> rpRecords = records.records(rp);
                        if (!rpRecords.isEmpty()) {
                            for (ConsumerRecord<String, String> record : rpRecords) {
                                DelayMessage msg = gson.fromJson(record.value(), DelayMessage.class);
                                /**
                                 * 这里存在的问题：poll 的消息，有的到达投递时间，有的未达到，未达到的会“放弃消费”，后续会重新拉取
                                 */
                                if (msg.getDeliverTs() > curTs) {
                                    log.info("pause consume partitions {} msg {}", rp, record.value());
                                    pausePartitionMap.put(rp, msg.getDeliverTs());
                                    // 重置消费位移量
                                    // consumer.seek(curRecordPartition, record.offset());
                                    // pause 对应分区
                                    consumer.pause(List.of(rp));
                                    break;
                                } else {
                                    // todo consume
                                    log.info("consume msg {}", record.value());
                                    consumer.commitSync(Collections.singletonMap(rp, new OffsetAndMetadata(record.offset() + 1, null)));
                                }
                            }
                        } else {
                            log.info("no message in partition {}", rp);
                        }
                    }
                } else {
                    log.info("poll empty");
                }
            } catch (Exception e) {
                // todo log
            }
        }
    }

}
