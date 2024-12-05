package org.example;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBFactory;
import site.ycsb.StringByteIterator;
import site.ycsb.measurements.Measurements;
import org.apache.htrace.core.Tracer;
import org.apache.htrace.core.Tracer.Builder;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.util.*;
import java.io.*;

/**
 * BenchmarkTest is a benchmarking tool that evaluates the performance of Redis and Memcached
 * using the YCSB (Yahoo Cloud Serving Benchmark) framework. It measures the time taken for
 * INSERT, READ, UPDATE, DELETE operations on datasets of varying sizes and records the results in a CSV file.
 * It also performs custom queries with filters based on the data structure.
 */
public class BenchmarkTest {

    // Define the dataset sizes to be tested
    private static final int[] DATASET_SIZES = {1000, 10000, 100000,1000000};

    // Path to the output CSV file
    private static final String OUTPUT_CSV_FILE = "output/benchmark_results.csv";

    public static void main(String[] args) throws Exception {
        System.out.println("Current Working Directory: " + System.getProperty("user.dir"));

        // Ensure the output directory exists
        File outputDir = new File("output");
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }

        // Configure Log4j using the properties file
        PropertyConfigurator.configure(BenchmarkTest.class.getClassLoader().getResource("log4j.properties"));

        // Set the logging level for memcached to ERROR to suppress INFO logs
        Logger.getLogger("net.spy.memcached").setLevel(org.apache.log4j.Level.ERROR);

        // Initialize the CSV writer
        CSVWriter csvWriter = new CSVWriter(OUTPUT_CSV_FILE);
        csvWriter.writeHeader("Database", "Dataset Size", "Operation", "Time (ms)");

        // Start benchmarking for Redis
        System.out.println("\n===============================================");
        System.out.println("           Starting Redis Benchmark");
        System.out.println("===============================================\n");

        // Iterate through each dataset size for Redis
        for (int size : DATASET_SIZES) {
            // Print a clear separator and the current dataset size
            System.out.println("\n-----------------------------------------------");
            System.out.println("Testing with dataset size: " + size);
            System.out.println("-----------------------------------------------");

            // Initialize Redis-specific properties
            Properties redisProps = initializeRedisProperties(size);

            // Run the benchmark for Redis and record the results
            runBenchmark(redisProps, size, "Redis", csvWriter);
        }

        // Start benchmarking for Memcached
        System.out.println("\n===============================================");
        System.out.println("        Starting Memcached Benchmark");
        System.out.println("===============================================\n");

        // Iterate through each dataset size for Memcached
        for (int size : DATASET_SIZES) {
            // Print a clear separator and the current dataset size
            System.out.println("\n-----------------------------------------------");
            System.out.println("Testing with dataset size: " + size);
            System.out.println("-----------------------------------------------");

            // Initialize Memcached-specific properties
            Properties memcachedProps = initializeMemcachedProperties(size);

            // Run the benchmark for Memcached and record the results
            runBenchmark(memcachedProps, size, "Memcached", csvWriter);
        }

        // Indicate that benchmarking is complete
        System.out.println("\n===============================================");
        System.out.println("               Benchmark Complete");
        System.out.println("===============================================\n");

        // Close the CSV writer to ensure all data is flushed and resources are released
        csvWriter.close();
    }

    /**
     * Initializes the properties required for Redis benchmarking.
     *
     * @param size The size of the dataset to be used.
     * @return A Properties object containing Redis configuration.
     */
    private static Properties initializeRedisProperties(int size) {
        Properties props = new Properties();
        props.setProperty("db", "site.ycsb.db.RedisClient"); // Specify the Redis client
        props.setProperty("redis.host", "localhost");       // Redis server host
        props.setProperty("redis.port", "6379");            // Redis server port
        props.setProperty("recordcount", String.valueOf(size));      // Number of records to insert
        props.setProperty("operationcount", String.valueOf(size));   // Number of operations to perform
        props.setProperty("workload", "site.ycsb.workloads.CoreWorkload"); // Specify the workload type
        return props;
    }

    /**
     * Initializes the properties required for Memcached benchmarking.
     *
     * @param size The size of the dataset to be used.
     * @return A Properties object containing Memcached configuration.
     */
    private static Properties initializeMemcachedProperties(int size) {
        Properties props = new Properties();
        props.setProperty("db", "site.ycsb.db.MemcachedClient"); // Specify the Memcached client
        props.setProperty("memcached.hosts", "localhost:11211"); // Memcached server address
        props.setProperty("recordcount", String.valueOf(size));      // Number of records to insert
        props.setProperty("operationcount", String.valueOf(size));   // Number of operations to perform
        props.setProperty("workload", "site.ycsb.workloads.CoreWorkload"); // Specify the workload type
        return props;
    }

    /**
     * Executes the benchmarking process for a given database configuration and records the results.
     *
     * @param props       The properties/configuration for the database.
     * @param datasetSize The size of the dataset to be used.
     * @param dbName      The name of the database (e.g., Redis, Memcached).
     * @param csvWriter   The CSVWriter instance to record the results.
     * @throws Exception If an error occurs during benchmarking.
     */
    private static void runBenchmark(Properties props, int datasetSize, String dbName, CSVWriter csvWriter) throws Exception {
        // Set the benchmarking properties
        Measurements.setProperties(props);

        // Initialize the tracer (NoopTracer means no actual tracing)
        Tracer tracer = new Builder("NoopTracer").build();

        // Create a new database instance based on the provided properties
        DB db = DBFactory.newDB(props.getProperty("db"), props, tracer);

        // Initialize the database (establish connections, etc.)
        db.init();

        // Load the dataset from the JSON file
        List<Map<String, ByteIterator>> dataset = loadDataset(datasetSize);

        // Perform the insertion benchmark and record the result
        long insertTime = performInsertionBenchmark(db, dataset, dbName);
        csvWriter.writeRecord(dbName, String.valueOf(datasetSize), "INSERT", String.valueOf(insertTime));

        // Perform the read benchmark and record the result
        long readTime = performReadBenchmark(db, dataset, dbName);
        csvWriter.writeRecord(dbName, String.valueOf(datasetSize), "READ", String.valueOf(readTime));

        // Perform the update benchmark and record the result
        long updateTime = performUpdateBenchmark(db, dataset, dbName);
        csvWriter.writeRecord(dbName, String.valueOf(datasetSize), "UPDATE", String.valueOf(updateTime));

        // Perform the delete benchmark and record the result
        long deleteTime = performDeleteBenchmark(db, dataset, dbName);
        csvWriter.writeRecord(dbName, String.valueOf(datasetSize), "DELETE", String.valueOf(deleteTime));

        // Perform custom queries and record the results
        long customQueryTime = performCustomQuery(db, dataset, dbName);
        csvWriter.writeRecord(dbName, String.valueOf(datasetSize), "CUSTOM_QUERY", String.valueOf(customQueryTime));

        // Clean up the database connections and resources
        db.cleanup();
    }

    /**
     * Performs the insertion benchmark by inserting records into the database.
     *
     * @param db      The database instance.
     * @param dataset The dataset containing records to insert.
     * @param dbName  The name of the database (for logging purposes).
     * @return The time taken for the insertion process in milliseconds.
     * @throws Exception If an error occurs during insertion.
     */
    private static long performInsertionBenchmark(DB db, List<Map<String, ByteIterator>> dataset, String dbName) throws Exception {
        // Log the start of the INSERT benchmark
        System.out.println("\n*** Testing INSERT operation on " + dbName + " ***");

        // Record the start time of the insertion process
        long insertStartTime = System.currentTimeMillis();

        // Iterate through each record in the dataset and insert it into the database
        for (int i = 0; i < dataset.size(); i++) {
            String key = "record" + i; // Generate a unique key for each record
            db.insert("usertable", key, dataset.get(i)); // Insert the record
        }

        // Record the end time of the insertion process
        long insertEndTime = System.currentTimeMillis();

        // Calculate the total time taken for insertion
        long insertionTime = insertEndTime - insertStartTime;

        // Log the total time taken for insertion
        System.out.println("Insertion time: " + insertionTime + " ms");

        return insertionTime;
    }

    /**
     * Performs the read benchmark by reading records from the database.
     *
     * @param db      The database instance.
     * @param dataset The dataset containing records to read.
     * @param dbName  The name of the database (for logging purposes).
     * @return The time taken for the read process in milliseconds.
     * @throws Exception If an error occurs during reading.
     */
    private static long performReadBenchmark(DB db, List<Map<String, ByteIterator>> dataset, String dbName) throws Exception {
        // Log the start of the READ benchmark
        System.out.println("\n*** Testing READ operation on " + dbName + " ***");

        // Record the start time of the read process
        long readStartTime = System.currentTimeMillis();

        // Iterate through each record in the dataset and read it from the database
        for (int i = 0; i < dataset.size(); i++) {
            String key = "record" + i; // Generate the key corresponding to the record
            Set<String> fields = null; // Specify null to read all fields
            HashMap<String, ByteIterator> result = new HashMap<>(); // Container to hold the read result
            db.read("usertable", key, fields, result); // Perform the read operation
        }

        // Record the end time of the read process
        long readEndTime = System.currentTimeMillis();

        // Calculate the total time taken for reading
        long readTime = readEndTime - readStartTime;

        // Log the total time taken for reading
        System.out.println("Read time: " + readTime + " ms");

        return readTime;
    }

    /**
     * Performs the update benchmark by updating records in the database.
     *
     * @param db      The database instance.
     * @param dataset The dataset containing records to update.
     * @param dbName  The name of the database (for logging purposes).
     * @return The time taken for the update process in milliseconds.
     * @throws Exception If an error occurs during updating.
     */
    private static long performUpdateBenchmark(DB db, List<Map<String, ByteIterator>> dataset, String dbName) throws Exception {
        // Log the start of the UPDATE benchmark
        System.out.println("\n*** Testing UPDATE operation on " + dbName + " ***");

        // Record the start time of the update process
        long updateStartTime = System.currentTimeMillis();

        // Prepare the values to update (e.g., increment 'bytes' field by 1000)
        for (int i = 0; i < dataset.size(); i++) {
            String key = "record" + i; // Generate the key corresponding to the record
            Map<String, ByteIterator> valuesToUpdate = new HashMap<>();

            // Read the existing record
            HashMap<String, ByteIterator> existingRecord = new HashMap<>();
            db.read("usertable", key, null, existingRecord);

            // Get the 'bytes' field or assign default value if missing
            String bytesStr = "0.0"; // Default value
            ByteIterator bytesBI = existingRecord.get("bytes");
            if (bytesBI != null) {
                bytesStr = bytesBI.toString();
            }

            double bytesValue = 0.0;
            try {
                bytesValue = Double.parseDouble(bytesStr) + 1000.0;
            } catch (NumberFormatException e) {
                // Assign default value if parsing fails
                System.err.println("Number format error for 'bytes' in key: " + key + ", assigning default value 0.0");
                bytesValue = 1000.0; // Since we're adding 1000, start from 0
            }

            // Store the updated value
            valuesToUpdate.put("bytes", new StringByteIterator(String.valueOf(bytesValue)));

            // Perform the update operation
            db.update("usertable", key, valuesToUpdate);
        }

        // Record the end time of the update process
        long updateEndTime = System.currentTimeMillis();

        // Calculate the total time taken for updating
        long updateTime = updateEndTime - updateStartTime;

        // Log the total time taken for updating
        System.out.println("Update time: " + updateTime + " ms");

        return updateTime;
    }

    /**
     * Performs the delete benchmark by deleting records from the database.
     *
     * @param db      The database instance.
     * @param dataset The dataset containing records to delete.
     * @param dbName  The name of the database (for logging purposes).
     * @return The time taken for the delete process in milliseconds.
     * @throws Exception If an error occurs during deletion.
     */
    private static long performDeleteBenchmark(DB db, List<Map<String, ByteIterator>> dataset, String dbName) throws Exception {
        // Log the start of the DELETE benchmark
        System.out.println("\n*** Testing DELETE operation on " + dbName + " ***");

        // Record the start time of the delete process
        long deleteStartTime = System.currentTimeMillis();

        // Iterate through each record and delete it from the database
        for (int i = 0; i < dataset.size(); i++) {
            String key = "record" + i; // Generate the key corresponding to the record
            db.delete("usertable", key); // Perform the delete operation
        }

        // Record the end time of the delete process
        long deleteEndTime = System.currentTimeMillis();

        // Calculate the total time taken for deletion
        long deleteTime = deleteEndTime - deleteStartTime;

        // Log the total time taken for deletion
        System.out.println("Delete time: " + deleteTime + " ms");

        return deleteTime;
    }

    /**
     * Performs a custom query by filtering records based on certain criteria.
     * Since Redis and Memcached are key-value stores, complex queries are not natively supported.
     * We'll simulate the query by reading all records and applying the filter in the application layer.
     * Example Query: Find all records where 'http_reply_code' is 200 and 'bytes' > 5000.
     *
     * @param db      The database instance.
     * @param dataset The dataset containing records.
     * @param dbName  The name of the database (for logging purposes).
     * @return The time taken for the custom query in milliseconds.
     * @throws Exception If an error occurs during the query.
     */
    private static long performCustomQuery(DB db, List<Map<String, ByteIterator>> dataset, String dbName) throws Exception {
        // Log the start of the custom query
        System.out.println("\n*** Testing CUSTOM QUERY on " + dbName + " ***");
        System.out.println("Query: Find records where 'http_reply_code' is 200 and 'bytes' > 5000");

        // Record the start time of the query
        long queryStartTime = System.currentTimeMillis();

        // Container to hold the keys that match the criteria
        List<String> matchingKeys = new ArrayList<>();

        // Iterate through each record and apply the filter
        for (int i = 0; i < dataset.size(); i++) {
            String key = "record" + i;
            HashMap<String, ByteIterator> result = new HashMap<>();
            db.read("usertable", key, null, result);

            // Retrieve the ByteIterator objects
            ByteIterator replyCodeBI = result.get("http_reply_code");
            ByteIterator bytesBI = result.get("bytes");

            // Assign default values if fields are missing
            String replyCodeStr = "0"; // Default value
            String bytesStr = "0.0";   // Default value

            if (replyCodeBI != null) {
                replyCodeStr = replyCodeBI.toString();
            }

            if (bytesBI != null) {
                bytesStr = bytesBI.toString();
            }

            int replyCode = 0;
            double bytes = 0.0;

            try {
                // Parse the 'http_reply_code'
                replyCode = (int) Double.parseDouble(replyCodeStr);
            } catch (NumberFormatException e) {
                // Assign default value if parsing fails
                System.err.println("Number format error for 'http_reply_code' in key: " + key + ", assigning default value 0");
                replyCode = 0;
            }

            try {
                // Parse the 'bytes'
                bytes = Double.parseDouble(bytesStr);
            } catch (NumberFormatException e) {
                // Assign default value if parsing fails
                System.err.println("Number format error for 'bytes' in key: " + key + ", assigning default value 0.0");
                bytes = 0.0;
            }

            // Apply the filter criteria
            if (replyCode == 200 && bytes > 5000.0) {
                matchingKeys.add(key);
            }
        }

        // Record the end time of the query
        long queryEndTime = System.currentTimeMillis();

        // Calculate the total time taken for the query
        long queryTime = queryEndTime - queryStartTime;

        // Log the total time taken and number of matching records
        System.out.println("Custom query time: " + queryTime + " ms");
        System.out.println("Number of matching records: " + matchingKeys.size());

        return queryTime;
    }

    /**
     * Loads the dataset from a JSON file up to the specified size.
     *
     * @param size The number of records to load.
     * @return A list of maps where each map represents a record.
     * @throws IOException If an error occurs while reading the file.
     */
    private static List<Map<String, ByteIterator>> loadDataset(int size) throws IOException {
        List<Map<String, ByteIterator>> dataset = new ArrayList<>();

        // Attempt to load the JSON file from the classpath
        InputStream inputStream = BenchmarkTest.class.getClassLoader().getResourceAsStream("parsed_NASA_access_log.json");
        if (inputStream == null) {
            throw new FileNotFoundException("File not found: parsed_NASA_access_log.json");
        }

        // Read the JSON content
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        com.google.gson.Gson gson = new com.google.gson.Gson();
        Map<String, Object>[] jsonArray = gson.fromJson(reader, Map[].class);
        reader.close();

        // Determine the actual size based on the available data
        int actualSize = Math.min(size, jsonArray.length);

        // Convert each JSON object to a Map<String, ByteIterator>
        for (int i = 0; i < actualSize; i++) {
            Map<String, Object> jsonMap = jsonArray[i];
            Map<String, ByteIterator> entry = new HashMap<>();

            // Iterate through each key-value pair in the JSON object
            for (Map.Entry<String, Object> jsonEntry : jsonMap.entrySet()) {
                // Convert the value to a StringByteIterator and add it to the entry map
                entry.put(jsonEntry.getKey(), new StringByteIterator(jsonEntry.getValue().toString()));
            }

            // Add the entry to the dataset
            dataset.add(entry);
        }

        return dataset;
    }
}
