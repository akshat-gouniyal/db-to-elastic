package org.example;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;

public class SearchService {

    private static final String INDEX_NAME = "psql_migration"; // Your index name

    public static void searchDocuments(RestHighLevelClient client) throws IOException {
        // Start the timer
        long startTime = System.nanoTime(); // or System.currentTimeMillis() for milliseconds

        // Create a search request for the specified index
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        // Build a query to find documents where 'active' is false
        sourceBuilder.query(QueryBuilders.termQuery("active", false));
//        sourceBuilder.query(QueryBuilders.termQuery("src_table", "inventory_items"));

        searchRequest.source(sourceBuilder);

        // Execute the search request
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        // End the timer
        long endTime = System.nanoTime(); // or System.currentTimeMillis()

        // Calculate and print the elapsed time
        long duration = endTime - startTime;
        System.out.println("Search took: " + duration / 1_000_000 + " milliseconds"); // Convert to milliseconds

        // Process and print the search results
        long totalHits = searchResponse.getHits().getTotalHits().value;
        System.out.println("Total hits for active=false: " + totalHits);

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            System.out.println("Document ID: " + hit.getId());
            System.out.println("Document Source: " + hit.getSourceAsString());
        }
    }
}
