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
                        consumer.resume(List.of(pausePartition)); // (1)
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
                                    /**
                                     * 重置消费位移量！！！
                                     * 注：
                                     * 1、这里如果不重置，消费者此次生命周期内不会再拉到该消息；
                                     * 2、即使重启，也可能拉不到，因为后续消息的消费位移提交，会导致该消息“永久丢失”；
                                     */
                                    consumer.seek(rp, record.offset()); // (2)
                                    // pause 对应分区
                                    consumer.pause(List.of(rp)); // (3)
                                    break;
                                } else {
                                    log.info("consume msg {}", record.value());
                                    /**
                                     * 这里必须按单条消息提交消费位移！！！(todo 可以在 poll 批次内优化，但也是精确到某条消息！！！)
                                     * 注：这里涉及到 API 的具体行为：以无参 commitSync() 为例，默认提交拉取到的最后一条消息位移
                                     */
                                    consumer.commitSync(Collections.singletonMap(rp, new OffsetAndMetadata(record.offset() + 1, null))); // (4)
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
                log.warn("consume error", e);
            }
        }
    }

}
