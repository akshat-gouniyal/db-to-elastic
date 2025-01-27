package org.example;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.elasticsearch._types.Time;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {

    public static void main(String[] args) {
        // PostgreSQL connection details
        String jdbcUrl = "jdbc:postgresql://localhost:5432/product";  // Modify with your actual db details
        String jdbcUser = "postgres";
        String jdbcPassword = "Logik2021";

        // Elasticsearch connection details
        String esHost = "localhost";
        int esPort = 9200;
        String esIndex = "testjoinedtable";  // Your index name

        ObjectMapper objectMapper = new ObjectMapper();


        // Set pagination parameters
        int batchSize = 20000; // Number of records to fetch per batch
        long startTime = System.currentTimeMillis(); // Start timing

        // Max records variable; set to -1 for no limit or specify a number (e.g., 1000000)
        int maxRecords = 60000; // Change this value as needed (e.g., 1000000 for one million)

        try (
                Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
                RestClient restClient = RestClient.builder(new HttpHost(esHost, esPort)).build();
                RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
                ElasticsearchClient client = new ElasticsearchClient(transport)
        ) {
            // Delete existing Elasticsearch index (optional)
            try {
                client.indices().delete(DeleteIndexRequest.of(r -> r.index(esIndex)));
                System.out.println("Deleted existing index: " + esIndex);
            } catch (Exception e) {
                System.out.println("Index does not exist or could not be deleted: " + e.getMessage());
            }

            // Create a new Elasticsearch index
            try {
                client.indices().create(CreateIndexRequest.of(r -> r.index(esIndex)
                        .settings(s -> s
                                .numberOfShards("1") // Set number of shards
                                .numberOfReplicas("0") // Set replicas to zero initially as a string
                        )));
                System.out.println("Created new index: " + esIndex);
            } catch (Exception e) {
                throw new RuntimeException("Failed to create index: " + e.getMessage());
            }

            // Disable refresh interval for bulk indexing
            client.indices().putSettings(r -> r.index(esIndex)
                    .settings(s -> s.refreshInterval(Time.of(t -> t.time("-1")))));

            boolean moreRecords = true; // Flag to control the loop
            int offset = 0; // Starting point for fetching records
            int totalFetched = 0; // Counter for total records fetched

            while (moreRecords && (maxRecords == -1 || totalFetched < maxRecords)) {
                System.out.println("Starting batch with OFFSET: " + offset);

                // Query to fetch rows in ascending order of ID, limiting to remaining records needed
                String query = "SELECT\n" +
                        "        p.*, -- All fields from the product table\n" +
                        "        to_json(i) AS inventory_items_json,\n" +
                        "        to_json(k) AS ks_coreswduration_json,\n" +
                        "        to_json(l) AS largedatatable_json\n" +
                        "    FROM\n" +
                        "        product p\n" +
                        "    LEFT JOIN inventory_items i ON p.product_code = i.product_number\n" +
                        "    LEFT JOIN ks_coreswduration k ON p.product_code = k.hwmodel\n" +
                        "    LEFT JOIN largedatatable l ON p.product_code = l.product_code LIMIT " +
                        Math.min(batchSize, maxRecords == -1 ? Integer.MAX_VALUE : maxRecords - totalFetched) +
                        " OFFSET " + offset;

                System.out.println("Executing query: " + query); // Log the SQL query

                try (Statement statement = connection.createStatement();
                     ResultSet resultSet = statement.executeQuery(query)) {

                    int rowCount = 0; // Count rows fetched
                    List<BulkOperation> bulkOperations = new ArrayList<>(); // Create a list for bulk operations

                    while (resultSet.next()) {
                        rowCount++;
                        totalFetched++; // Increment total fetched count

                        Map<String, Object> documentMap = new HashMap<>();
                        ResultSetMetaData metaData = resultSet.getMetaData();
                        int columnCount = metaData.getColumnCount();

                        // Iterate through all columns dynamically
                        for (int i = 1; i <= columnCount; i++) {
                            String columnName = metaData.getColumnName(i);
                            Object columnValue = resultSet.getObject(i);

                            if (columnName.endsWith("_json") && columnValue != null) {
                                // Parse JSON string to JSON object
                                JsonNode jsonNode = objectMapper.readTree(columnValue.toString());
                                documentMap.put(columnName, jsonNode);
                            } else {
                                documentMap.put(columnName, columnValue); // Add the column value directly without parsing
                            }
                        }

                        // Use the 'id' column as the unique document ID
                        String documentId = resultSet.getString("id");

                        // Add each document to the bulk operations list
                        bulkOperations.add(BulkOperation.of(b -> b.index(idx -> idx
                                .index(esIndex)
                                .id(documentId)
                                .document(documentMap))));
                    }

                    if (!bulkOperations.isEmpty()) {
                        BulkRequest bulkRequest = BulkRequest.of(b -> b.operations(bulkOperations));
                        BulkResponse bulkResponse = client.bulk(bulkRequest);

                        if (bulkResponse.errors()) {
                            System.err.println("Bulk indexing had failures.");
                            bulkResponse.items().forEach(item -> {
                                if (item.error() != null) {
                                    System.err.println("Error indexing document ID: " + item.id() + ", Error: " + item.error().reason());
                                }
                            });
                        } else {
                            System.out.println("Successfully indexed batch with OFFSET: " + offset);
                        }
                    }

                    if (rowCount < batchSize || totalFetched >= maxRecords) {
                        moreRecords = false; // No more records to fetch or limit reached
                    }
                    offset += rowCount; // Update offset based on actual rows fetched

                } catch (SQLException e) {
                    System.err.println("Error while interacting with PostgreSQL: " + e.getMessage());
                }
            }

            // Re-enable refresh interval and restore replicas after indexing is complete
            client.indices().putSettings(r -> r.index(esIndex)
                    .settings(s -> s.refreshInterval(Time.of(t -> t.time("1s"))).numberOfReplicas("1"))); // Set replicas as a string

            long endTime = System.currentTimeMillis(); // End timing
            long duration = endTime - startTime; // Calculate duration in milliseconds

            System.out.println("Finished indexing records into Elasticsearch.");
            System.out.println("Total time taken: " + duration + " milliseconds (" + (duration / 1000.0) + " seconds)");
        } catch (IOException e) {
            System.err.println("Error while interacting with Elasticsearch: " + e.getMessage());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}