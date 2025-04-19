package io.conduktor.demos.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class ProducerDemoWithCallback {
    public static void main(String[] args) {
        log.info("Hello, World!");

        // 1. 프로펄티 생성하기
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());

        // 파티션 분배에 대한 기본값은 StickyPartitioner
//        properties.put("batch.size", 400);
//        properties.put("partitioner.class", RoundRobinPartitioner.class.getName());

        // 2. 프로듀서 생성하기
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 3. 데이터 보내기 -> 콜백
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("test-topic", "Hello, World! " + i);

            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e != null) {
                        log.error("Error while producing message", e);
                    } else {
                        log.info("Message sent successfully");
                        log.info("Topic: " + metadata.topic());
                        log.info("Partition: " + metadata.partition());
                        log.info("Offset: " + metadata.offset());
                        log.info("Timestamp: " + metadata.timestamp());
                    }
                }
            });
        }

        // 4. flush 하고 프로듀서 닫기
        producer.flush();
        producer.close();
    }
}
