package org.example;

import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

public class Main {

    public static void main(String[] args) {
        // Create an Elasticsearch client
        try (RestHighLevelClient client = ElasticSearchClient.createClient()) {

            // Call the search service
            SearchService.searchDocuments(client);

        } catch (IOException e) {
            System.err.println("Error while interacting with Elasticsearch: " + e.getMessage());
        }
    }
}
