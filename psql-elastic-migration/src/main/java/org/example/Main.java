package org.example;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

    public static void main(String[] args) {
        // PostgreSQL connection details
        String jdbcUrl = "jdbc:postgresql://localhost:5432/elasticproducttest";  // Modify with your actual db details
        String jdbcUser = "postgres";
        String jdbcPassword = "Logik2021";

        // Elasticsearch connection details
        String esHost = "localhost";
        int esPort = 9200;

        // Define table-to-index mapping
        String[][] tableToIndexMapping = {
                {"product", "psql_migration"},          // Table: product -> Index: psql_migration
                {"inventory_items", "psql_migration"}, // Table: inventory_items -> Index: psql_migration
                {"largedatatable", "psql_migration"},  // Table: largedatatable -> Index: psql_migration
                {"ks_coreswduration", "psql_migration"} // Table: ks_coreswduration -> Index: psql_migration
        };

        int batchSize = 10000; // Number of records to fetch per batch
        int maxRecords = -1; // Set to -1 for unlimited fetching

        ExecutorService executorService = Executors.newFixedThreadPool(tableToIndexMapping.length); // Create a thread pool

        for (String[] mapping : tableToIndexMapping) {
            String tableName = mapping[0]; // PostgreSQL table name
            String indexName = mapping[1]; // Elasticsearch index name

            executorService.submit(new TableIndexer(jdbcUrl, jdbcUser, jdbcPassword, esHost, esPort, tableName, indexName, batchSize, maxRecords));
        }

        executorService.shutdown(); // Shutdown the executor service after submitting all tasks

        System.out.println("All indexing tasks submitted.");
    }
}
