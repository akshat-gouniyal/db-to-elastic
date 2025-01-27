package org.example;

import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RestClient;
import org.apache.http.HttpHost;
import java.io.IOException;

public class ElasticSearchClient {

    private static final String ES_HOST = "localhost";
    private static final int ES_PORT = 9200;

    // Create and return an Elasticsearch client
    public static RestHighLevelClient createClient() {
        return new RestHighLevelClient(
                RestClient.builder(new HttpHost(ES_HOST, ES_PORT, "http"))
        );
    }

    // Close the client safely
    public static void closeClient(RestHighLevelClient client) {
        try {
            client.close();
        } catch (IOException e) {
            System.err.println("Error closing the Elasticsearch client: " + e.getMessage());
        }
    }
}
