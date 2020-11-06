package stream;

import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

public class ElasticsearchConsumer {

    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticsearchConsumer.class.getName());

        // create elastic client
        RestHighLevelClient client = createClient();

        // create kafka consumer
        KafkaConsumer<String, String> consumer = createConsumer();

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
