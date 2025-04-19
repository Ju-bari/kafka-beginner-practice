package io.conduktor.demos.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class ProducerDemoWithKey {
    public static void main(String[] args) {
        log.info("Hello, World!");

        // 1. 프로펄티 생성하기
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());

        // 2. 프로듀서 생성하기
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 3. 데이터 보내기 -> 콜백
        for (int j = 0; j < 2; j++) { // 동일 키 -> 동일 파티션 확인을 위해 두 번 진행
            for (int i = 0; i < 10; i++) {
                String topic = "test-topic";
                String key = "id_" + i;
                String value = "Hello, World! " + i;

                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e != null) {
                            log.error("Error while producing message", e);
                        } else {
                            log.info("=== Message sent successfully ===");
                            log.info("Key: " + key + " -> " + "Value: " + value);
                            log.info("Topic: " + metadata.topic());
                            log.info("Partition: " + metadata.partition());
                            log.info("Offset: " + metadata.offset());
                            log.info("Timestamp: " + metadata.timestamp());
                        }
                    }
                });
            }
        }
        // 4. flush 하고 프로듀서 닫기
        producer.flush();
        producer.close();
    }
}
