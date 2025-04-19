package io.conduktor.demos.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@Slf4j
public class ConsumerDemo {
    public static void main(String[] args) {
        String groupId = "java-app";
        String topic = "test-topic";

        // 1. 프로펄티 생성하기
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");

        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());
        properties.put("group.id", groupId);

        properties.put("auto.offset.reset", "earliest"); // none, earliest, latest
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");

        // 2. 컨슈머 생성하기
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // 3. 토픽 구독하기
        consumer.subscribe(Arrays.asList(topic));

        // 4. 데이터 가져오기
        while (true) {
            log.info("Polling data...");

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000)); // 없으면 1초 더 대기
            for (var record : records) {
                log.info("Key: " + record.key());
                log.info("Value: " + record.value());
                log.info("Partition: " + record.partition());
                log.info("Offset: " + record.offset());
                log.info("Timestamp: " + record.timestamp());
            }
        }

    }
}
