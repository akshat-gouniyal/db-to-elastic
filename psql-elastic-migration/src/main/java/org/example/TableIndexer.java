package org.example;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch._types.Time;
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

public class TableIndexer implements Runnable {
    private final String jdbcUrl;
    private final String jdbcUser;
    private final String jdbcPassword;
    private final String esHost;
    private final int esPort;
    private final String tableName; // PostgreSQL table name
    private final String indexName; // Elasticsearch index name
    private final int batchSize;
    private final int maxRecords;

    public TableIndexer(String jdbcUrl, String jdbcUser, String jdbcPassword, String esHost, int esPort,
                        String tableName, String indexName, int batchSize, int maxRecords) {
        this.jdbcUrl = jdbcUrl;
        this.jdbcUser = jdbcUser;
        this.jdbcPassword = jdbcPassword;
        this.esHost = esHost;
        this.esPort = esPort;
        this.tableName = tableName; // Set PostgreSQL table name
        this.indexName = indexName; // Set Elasticsearch index name
        this.batchSize = batchSize;
        this.maxRecords = maxRecords;
    }

    @Override
    public void run() {
        long startTime = System.currentTimeMillis(); // Start timing

        try (
                Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
                RestClient restClient = RestClient.builder(new HttpHost(esHost, esPort)).build();
                RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
                ElasticsearchClient client = new ElasticsearchClient(transport)
        ) {
            // Check if the index exists and create it if it does not
            if (!client.indices().exists(r -> r.index(indexName)).value()) {
                client.indices().create(c -> c.index(indexName));
                System.out.println("Index created: " + indexName);
            } else {
                System.out.println("Index already exists: " + indexName);
            }

            // Disable refresh interval for bulk indexing
            client.indices().putSettings(r -> r.index(indexName)
                    .settings(s -> s.refreshInterval(Time.of(t -> t.time("-1")))));

            boolean moreRecords = true; // Flag to control the loop
            int offset = 0; // Starting point for fetching records
            int totalFetched = 0; // Counter for total records fetched

            while (moreRecords) {
                System.out.println("Starting batch with OFFSET: " + offset + " for table: " + tableName);

                // Calculate limit based on maxRecords
                int limit = (maxRecords == -1) ? batchSize : Math.min(batchSize, maxRecords - totalFetched);

                // Query to fetch rows in ascending order of ID from the specified table
                String query = "SELECT * FROM " + tableName + " ORDER BY id ASC LIMIT " + limit +
                        " OFFSET " + offset;

                try (Statement statement = connection.createStatement();
                     ResultSet resultSet = statement.executeQuery(query)) {

                    List<BulkOperation> bulkOperations = new ArrayList<>(); // Create a list for bulk operations
                    int rowsFetchedInBatch = 0;

                    while (resultSet.next()) {
                        Map<String, Object> documentMap = new HashMap<>();
                        ResultSetMetaData metaData = resultSet.getMetaData();
                        int columnCount = metaData.getColumnCount();

                        // Iterate through all columns dynamically
                        for (int i = 1; i <= columnCount; i++) {
                            String columnName = metaData.getColumnName(i);
                            Object columnValue = resultSet.getObject(i);
                            documentMap.put(columnName, columnValue); // Add the column value directly without parsing
                        }

                        // Use the table name and original ID to create a unique document ID
                        String documentId = tableName + "_" + resultSet.getString("id");

                        // Add src_table attribute to indicate source table
                        documentMap.put("src_table", tableName); // Add source table attribute

                        // Add each document to the bulk operations list
                        bulkOperations.add(BulkOperation.of(b -> b.index(idx -> idx
                                .index(indexName) // Use Elasticsearch index name here
                                .id(documentId)   // Unique ID for each document
                                .document(documentMap))));

                        rowsFetchedInBatch++;
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

                    totalFetched += rowsFetchedInBatch; // Update total fetched count
                    offset += rowsFetchedInBatch;       // Update offset based on actual rows fetched

                    if (rowsFetchedInBatch < batchSize || (maxRecords != -1 && totalFetched >= maxRecords)) {
                        moreRecords = false; // No more records to fetch or limit reached
                    }

                } catch (SQLException e) {
                    System.err.println("Error while interacting with PostgreSQL: " + e.getMessage());
                }
            }

            System.out.println("Total records fetched and indexed from table: " + tableName + " = " + totalFetched);

            long endTime = System.currentTimeMillis(); // End timing
            long duration = endTime - startTime; // Calculate duration in milliseconds

            System.out.println("Finished indexing records from table: " + tableName +
                    " into index: " + indexName);
            System.out.println("Total time taken for indexing: " + duration + " milliseconds (" +
                    (duration / 1000.0) + " seconds)");
        } catch (IOException e) {
            System.err.println("Error while interacting with Elasticsearch: " + e.getMessage());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
