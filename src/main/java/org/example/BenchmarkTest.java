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
 * INSERT and READ operations on datasets of varying sizes and records the results in a CSV file.
 */
public class BenchmarkTest {

    // Define the dataset sizes to be tested
    private static final int[] DATASET_SIZES = {1000, 10000, 100000, 1000000};

    // Path to the output CSV file
    private static final String OUTPUT_CSV_FILE = "src/main/java/org/example/output/benchmark_results.csv";

    public static void main(String[] args) throws Exception {
        System.out.println("Current Working Directory: " + System.getProperty("user.dir"));

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
     * @param props      The properties/configuration for the database.
     * @param datasetSize The size of the dataset to be used.
     * @param dbName     The name of the database (e.g., Redis, Memcached).
     * @param csvWriter  The CSVWriter instance to record the results.
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
            String key = "user" + i; // Generate a unique key for each record
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
            String key = "user" + i; // Generate the key corresponding to the record
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
