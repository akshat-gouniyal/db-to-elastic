package org.example;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Testquery {

    private static final String JDBC_URL = "jdbc:postgresql://localhost:5432/elasticproducttest"; // Modify with your actual db details
    private static final String JDBC_USER = "postgres";
    private static final String JDBC_PASSWORD = "Logik2021";
    private static final String ES_HOST = "localhost";
    private static final int ES_PORT = 9200;
    private static final String INDEX_NAME = "testjoinedtable"; // Elasticsearch index name

    public static void main(String[] args) {
        int batchSize = 10000; // Number of records to fetch per batch
        int maxRecords = 100000; // Set the maximum number of records to retrieve (modify this as needed)
        int offset = 0; // Starting point for fetching records

        long overallStartTime = System.currentTimeMillis(); // Start timing for overall execution

        try (
                RestClient restClient = RestClient.builder(new HttpHost(ES_HOST, ES_PORT)).build();
                RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
                ElasticsearchClient client = new ElasticsearchClient(transport);
                Connection connection = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD)
        ) {
            // Check if the index exists and create it if it does not
            if (!client.indices().exists(r -> r.index(INDEX_NAME)).value()) {
                client.indices().create(c -> c.index(INDEX_NAME));
                System.out.println("Index created: " + INDEX_NAME);
            } else {
                System.out.println("Index already exists: " + INDEX_NAME);
            }

            // Prepare the SQL query with placeholders for LIMIT and OFFSET
            String sqlQuery = "SELECT " +
                    "p.*, " +
                    "to_json(i) AS inventory_items_json, " +
                    "to_json(k) AS ks_coreswduration_json, " +
                    "to_json(l) AS largedatatable_json " +
                    "FROM product p " +
                    "LEFT JOIN inventory_items i ON p.product_code = i.product_number " +
                    "LEFT JOIN ks_coreswduration k ON p.product_code = k.hwmodel " +
                    "LEFT JOIN LargeDataTable l ON p.product_code = l.product_code " +
                    "LIMIT ? OFFSET ?";

            PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery);

            while (offset < maxRecords) { // Loop until we reach the maximum number of records
                // Set the LIMIT and OFFSET values
                preparedStatement.setInt(1, Math.min(batchSize, maxRecords - offset)); // Limit to remaining records
                preparedStatement.setInt(2, offset);

                // Execute the query
                ResultSet resultSet = preparedStatement.executeQuery();

                // Check if there are any results
                if (!resultSet.next()) {
                    break; // Exit if no more records are found
                }

                // Create a bulk request for Elasticsearch
                List<BulkOperation> bulkOperations = new ArrayList<>();

                do {
                    Map<String, Object> documentMap = new HashMap<>();

                    // Get metadata for columns dynamically
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    int columnCount = metaData.getColumnCount();

                    // Populate document map dynamically with result set data
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnName(i);
                        Object columnValue = resultSet.getObject(i); // Get value as Object for any type

                        documentMap.put(columnName, columnValue);
                    }

                    // Use product_code as a unique ID for each document
                    String documentId = resultSet.getString("product_code") + "_" + offset;

                    // Add each document to the bulk operations list
                    bulkOperations.add(BulkOperation.of(b -> b.index(idx -> idx
                            .index(INDEX_NAME) // Use Elasticsearch index name here
                            .id(documentId)     // Unique ID for each document
                            .document(documentMap))));

                } while (resultSet.next());

                if (!bulkOperations.isEmpty()) {
                    BulkRequest bulkRequest = BulkRequest.of(b -> b.operations(bulkOperations));
                    BulkResponse bulkResponse = client.bulk(bulkRequest);

                    if (bulkResponse.errors()) {
                        System.err.println("Bulk indexing had failures.");
                        bulkResponse.items().forEach(item -> {
                            if (item.error() != null) {
                                System.err.println("Error indexing document ID: "
                                        + item.id() + ", Error: "
                                        + item.error().reason());
                            }
                        });
                    } else {
                        System.out.println("Successfully indexed batch with OFFSET: "
                                + offset);
                    }
                }

                offset += batchSize; // Update offset for the next batch
            }

            preparedStatement.close();
            connection.close();

            long overallEndTime = System.currentTimeMillis(); // End timing for overall execution
            System.out.println("All records have been indexed successfully.");
            System.out.println("Total time taken for indexing: "
                    + (overallEndTime - overallStartTime) + " milliseconds");

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("Error while interacting with Elasticsearch: "
                    + e.getMessage());
        }
    }
}
