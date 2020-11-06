package stream;

import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticsearchConsumer {

    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticsearchConsumer.class.getName());

        // create elastic client
        RestHighLevelClient client = createClient();

        // create kafka consumer
        KafkaConsumer<String, String> consumer = createConsumer();


        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            BulkRequest bulkRequest = new BulkRequest();

            for (ConsumerRecord<String, String> record : records) {
                // kafka generic id to prevent document duplication
                String id = record.topic() + "_" + record.partition() + '_' + record.offset();
                logger.info("Consumer reads: " + record.value());

                try {
                    // insert data into elasticsearch
                    IndexRequest indexRequest = new IndexRequest("machine-telemetry")
                            .id(id)
                            .source(record.value(), XContentType.JSON);

                    bulkRequest.add(indexRequest);
                } catch (NullPointerException e) { // skip bad data
                    logger.warn("Skipping bad data: " + record.value());
                }
            }

            if (records.count() > 0) {
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                consumer.commitSync();
            }
        }

    }

    public static KafkaConsumer<String, String> createConsumer() throws IOException {
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "voltage-reading-elasticsearch";

        String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
        String propsPath = rootPath + "config.properties";

        // consumer configs
        FileInputStream in = new FileInputStream(propsPath);
        Properties properties = new Properties();
        properties.load(in);
        String topic = properties.getProperty("topic");
        in.close();

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }

    public static RestHighLevelClient createClient() {
        String hostName = "localhost";
        int port = 9200;

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostName, port, "http"));
        RestHighLevelClient client = new RestHighLevelClient(builder);

        return client;
    }
}
