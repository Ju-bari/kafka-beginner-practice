package io.conduktor.demos.kafka;

import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class OpenSearchConsumerWithCommitStrategies {
    public static void main(String[] args) throws IOException {

        // 1. create an OpenSearch Client -> RestHighLevelClient
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        // 2. create Kafka Client -> KafkaConsumer
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        // create index -> CreateIndexRequest
        try(openSearchClient; consumer) { // 두 가지 객체 전달 가능
            String indexName = "wikimedia";

            // 인덱스 존재여부 파악 먼저
            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);

            if (!indexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            } else {
                log.info("Index {} already exists", indexName);
            }

            // 데이터 컨슈밍 -> ConsumerRecords
            consumer.subscribe(Collections.singletonList("wikimedia.recentchange")); // Topic Name

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(3000);

                int recordsCount = records.count();
                log.info("{} records found", recordsCount);

                // from consumer to opensearch -> IndexRequest, IndexResponse
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        // idempotent를 위한 아이디 사용
                        String recordId = extractId(record.value());

                        // request
                        IndexRequest indexRequest = new IndexRequest("wikimedia")
                                .source(record.value(), XContentType.JSON)
                                .id(recordId); // idempotent

                        openSearchClient.index(indexRequest, RequestOptions.DEFAULT);

                        // response
                        IndexResponse indexResponse = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);

                        log.info("Index {} successfully added!", indexName);
                        log.info("response id: {}", indexResponse.getId()); // GET/wikimedia/_doc/<id>로 조회 가능
                    } catch (Exception e) {

                    }
                }

                // commit offsets after the batch it consumed
                consumer.commitAsync(); // 전부 다 하고 commit 하니 at least once 전략
                log.info("Consumer offsets committed");
            }

        } // 성공이든 실패든 실행 후 close 효과가 생김

        // 3. main logic

        // 4. close

    }

    public static String extractId(String json) {
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";

        // URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);

        // extract login info if it exits
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));
        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpClientBuilder
                                            -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)));


        } // setKeepAliveStrategy를 적을 곳이 없음

        return restHighLevelClient;
    }

    public static KafkaConsumer<String, String> createKafkaConsumer() {
        String bootstrapServers = "localhost:9092";
        String groupId = "opensearch-test-group";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); // 변경
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // 변경

        return new KafkaConsumer<>(props);
    }

}
