package io.conduktor.demos.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class ProducerDemo {
    public static void main(String[] args) {
        log.info( "Hello, World!");

        // 1. 프로펄티 생성하기
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());

        // 2. 프로듀서 생성하기
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("test-topic", "Hello, World!");

        // 3. 데이터 보내기
        producer.send(producerRecord); // 비동기...

        // 4. flush 하고 프로듀서 닫기
        producer.flush(); // 버퍼 단위를 무시하고 전송한다.
        producer.close();
    }
}
