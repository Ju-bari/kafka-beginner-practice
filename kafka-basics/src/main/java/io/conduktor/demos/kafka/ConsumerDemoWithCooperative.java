package io.conduktor.demos.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@Slf4j
public class ConsumerDemoWithCooperative {
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

        // 추가
        properties.put("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");

        // 2. 컨슈머 생성하기
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // 셧다운 훅 추가하기
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown. exit by calling consumer.wakeup()...");
                consumer.wakeup(); // 이후 poll()에서 예외 발생

                // 메인 스레드가 나머지를 실행하도록 함
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });


        try {
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
        } catch (WakeupException e) {
            log.info("WakeupException! Shutting down...");
        } catch (Exception e) {
            log.info("Unexpected exception!");
        } finally {
            consumer.close();
            log.info("Shutting down... done..!");
        }
    }

}