package stream;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticsearchConsumer {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ElasticsearchConsumer.class.getName());

        // create elastic client
        RestHighLevelClient client = createClient();

        // create kafka consumer

    }

    public static RestHighLevelClient createClient() {
        String hostName = "localhost";
        int port = 9200;

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostName, port, "http"));
        RestHighLevelClient client = new RestHighLevelClient(builder);

        return client;
    }
}
