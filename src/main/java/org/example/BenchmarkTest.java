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
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 * BenchmarkTest is a benchmarking tool that evaluates the performance of Redis and Memcached
 * using the YCSB framework. It measures INSERT, READ, UPDATE, DELETE operations and records the results.
 * It also performs a custom query at the end. After finishing each dataset size test,
 * it clears both Redis and Memcached databases, ensuring a clean state before testing the next dataset size.
 *
 * Key points:
 * - We serialize all fields of each record into a single JSON (stored in field0).
 * - We use a consistent key prefix (e.g., "ab-") so that keys match the naming convention used by Memcached.
 * - After each dataset size's benchmark is completed, we flush both databases.
 */

public class BenchmarkTest {

    // Define the dataset sizes to be tested.
    private static final int[] DATASET_SIZES = {1000, 10000, 100000, 1000000};
    // Output CSV file for benchmark results
    private static final String OUTPUT_CSV_FILE = "output/benchmark_results.csv";

    // Original fields from the dataset
    private static final String[] FIELD_NAMES = {"host", "timestamp", "request", "http_reply_code", "bytes"};

    // We only store JSON in field0 for each record
    private static final Set<String> FIELDS = new HashSet<>(Collections.singletonList("field0"));

    // Define a prefix for keys so that both Redis and Memcached behave consistently.
    private static final String KEY_PREFIX = "ab-";

    public static void main(String[] args) throws Exception {
        System.out.println("Starting BenchmarkTest...");
        clearRedis();
        clearMemcached();
        System.out.println("Current Working Directory: " + System.getProperty("user.dir"));

        File outputDir = new File("output");
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }

        // Configure log4j for logging because YCSB uses log4j for logging
        PropertyConfigurator.configure(BenchmarkTest.class.getClassLoader().getResource("log4j.properties"));
        // sets the logging level for the logger associated with the package net.spy.memcached to ERROR
        Logger.getLogger("net.spy.memcached").setLevel(org.apache.log4j.Level.ERROR);

        // Initialize CSV writer for storing benchmark results
        CSVWriter csvWriter = new CSVWriter(OUTPUT_CSV_FILE);
        csvWriter.writeHeader("Database", "Dataset Size", "Operation", "Time (ms)");

        // -------------------------------------
        // Redis Benchmark
        // -------------------------------------
        System.out.println("\n===============================================");
        System.out.println("           Starting Redis Benchmark");
        System.out.println("===============================================\n");

        for (int size : DATASET_SIZES) {
            System.out.println("\n-----------------------------------------------");
            System.out.println("Testing with dataset size: " + size);
            System.out.println("-----------------------------------------------");

            // Initialize properties for Redis
            Properties redisProps = initializeRedisProperties(size);

            // Run the benchmark and write results to the CSV file
            runBenchmark(redisProps, size, "Redis", csvWriter);

            // Clear Redis database after each test
            clearRedis();
        }

        // -------------------------------------
        // Memcached Benchmark
        // -------------------------------------
        System.out.println("\n===============================================");
        System.out.println("        Starting Memcached Benchmark");
        System.out.println("===============================================\n");

        for (int size : DATASET_SIZES) {
            System.out.println("\n-----------------------------------------------");
            System.out.println("Testing with dataset size: " + size);
            System.out.println("-----------------------------------------------");

            Properties memcachedProps = initializeMemcachedProperties(size);
            runBenchmark(memcachedProps, size, "Memcached", csvWriter);

            clearMemcached();
        }

        System.out.println("\n===============================================");
        System.out.println("               Benchmark Complete");
        System.out.println("===============================================\n");

        csvWriter.close();
    }

    /**
     * Initialize properties for Redis.
     */
    private static Properties initializeRedisProperties(int size) {
        Properties props = new Properties();
        props.setProperty("db", "site.ycsb.db.RedisClient");
        props.setProperty("redis.host", "localhost");
        props.setProperty("redis.port", "6379");
        props.setProperty("recordcount", String.valueOf(size));
        props.setProperty("operationcount", String.valueOf(size));
        props.setProperty("workload", "site.ycsb.workloads.CoreWorkload");
        return props;
    }

    /**
     * Initialize properties for Memcached.
     */
    private static Properties initializeMemcachedProperties(int size) {
        Properties props = new Properties();
        props.setProperty("db", "site.ycsb.db.MemcachedClient");
        props.setProperty("memcached.hosts", "localhost:11211");
        props.setProperty("recordcount", String.valueOf(size));
        props.setProperty("operationcount", String.valueOf(size));
        props.setProperty("workload", "site.ycsb.workloads.CoreWorkload");
        return props;
    }

    /**
     * Run the benchmark: Insert, Read, Update, Custom Query, and Delete operations.
     * Writes results to the CSV file.
     */
    private static void runBenchmark(Properties props, int datasetSize, String dbName, CSVWriter csvWriter) throws Exception {
        // Set fieldcount for YCSB measurement  ---> 5 fields because we have 5 fields in our dataset
        props.setProperty("fieldcount", "5");

        // Set properties for YCSB measurements (time taken for each operation)
        Measurements.setProperties(props);

        // tracer to measure the performance
        // Nooptracer --> trace or monitor individual operations, such as logs, spans, or distributed traces,
        // simply fulfills the requirement for a Tracer object where one is expected (like YCSB’s API) without doing anything.
        Tracer tracer = new Builder("NoopTracer").build();
        DB db = DBFactory.newDB(props.getProperty("db"), props, tracer);

        db.init();

        // Load dataset from a JSON file
        List<Map<String, ByteIterator>> dataset = loadDataset(datasetSize);

        // Perform operations and record times
        long insertTime = performInsertionBenchmark(db, dataset, dbName);
        csvWriter.writeRecord(dbName, String.valueOf(datasetSize), "INSERT", String.valueOf(insertTime));

        long readTime = performReadBenchmark(db, dataset, dbName);
        csvWriter.writeRecord(dbName, String.valueOf(datasetSize), "READ", String.valueOf(readTime));

        long updateTime = performUpdateBenchmark(db, dataset, dbName);
        csvWriter.writeRecord(dbName, String.valueOf(datasetSize), "UPDATE", String.valueOf(updateTime));

        long customQueryTime = performCustomQuery(db, dataset, dbName);
        csvWriter.writeRecord(dbName, String.valueOf(datasetSize), "CUSTOM_QUERY", String.valueOf(customQueryTime));

        long deleteTime = performDeleteBenchmark(db, dataset, dbName);
        csvWriter.writeRecord(dbName, String.valueOf(datasetSize), "DELETE", String.valueOf(deleteTime));

        db.cleanup();
    }

    /**
     * Insert all fields of each record as a single JSON object in field0.
     */
    private static long performInsertionBenchmark(DB db, List<Map<String, ByteIterator>> dataset, String dbName) throws Exception {
        System.out.println("\n*** Testing INSERT operation on " + dbName + " ***");
        Gson gson = new Gson();
        long insertStartTime = System.currentTimeMillis();

        for (int i = 0; i < dataset.size(); i++) {
            String key = KEY_PREFIX + "record" + i;
            Map<String, ByteIterator> originalEntry = dataset.get(i);

            Map<String, String> dataMap = new HashMap<>();
            for (String fname : FIELD_NAMES) {
                ByteIterator val = originalEntry.get(fname);
                dataMap.put(fname, val != null ? val.toString() : "0");
            }

            String jsonValue = gson.toJson(dataMap);
            Map<String, ByteIterator> ycsbEntry = new HashMap<>();
            ycsbEntry.put("field0", new StringByteIterator(jsonValue));

            db.insert("usertable", key, ycsbEntry);
        }

        long insertEndTime = System.currentTimeMillis();
        long insertionTime = insertEndTime - insertStartTime;
        System.out.println("Insertion time: " + insertionTime + " ms");
        return insertionTime;
    }



    private static long performReadBenchmark(DB db, List<Map<String, ByteIterator>> dataset, String dbName) throws Exception {
        System.out.println("\n*** Testing READ operation on " + dbName + " ***");
        long readStartTime = System.currentTimeMillis();

        for (int i = 0; i < dataset.size(); i++) {
            String key = KEY_PREFIX + "record" + i;
            HashMap<String, ByteIterator> result = new HashMap<>();
            db.read("usertable", key, FIELDS, result);

            // Even if we don't print them here, we deserialize to ensure correctness
            Map<String, String> dataMap = deserializeFields(result);
        }

        long readEndTime = System.currentTimeMillis();
        long readTime = readEndTime - readStartTime;
        System.out.println("Read time: " + readTime + " ms");
        return readTime;
    }

    private static long performUpdateBenchmark(DB db, List<Map<String, ByteIterator>> dataset, String dbName) throws Exception {
        System.out.println("\n*** Testing UPDATE operation on " + dbName + " ***");
        Gson gson = new Gson();
        long updateStartTime = System.currentTimeMillis();

        for (int i = 0; i < dataset.size(); i++) {
            String key = KEY_PREFIX + "record" + i;
            HashMap<String, ByteIterator> existingRecord = new HashMap<>();
            db.read("usertable", key, FIELDS, existingRecord);

            Map<String, String> dataMap = deserializeFields(existingRecord);
            String bytesStr = dataMap.getOrDefault("bytes", "0.0");

            double bytesValue = 0.0;
            try {
                bytesValue = Double.parseDouble(bytesStr) + 1000.0;
            } catch (NumberFormatException e) {
                System.err.println("Number format error for 'bytes' in key: " + key + ", defaulting to 1000.0");
                bytesValue = 1000.0;
            }

            dataMap.put("bytes", String.valueOf(bytesValue));
            String newJson = gson.toJson(dataMap);
            Map<String, ByteIterator> valuesToUpdate = new HashMap<>();
            valuesToUpdate.put("field0", new StringByteIterator(newJson));
            db.update("usertable", key, valuesToUpdate);
        }

        long updateEndTime = System.currentTimeMillis();
        long updateTime = updateEndTime - updateStartTime;
        System.out.println("Update time: " + updateTime + " ms");
        return updateTime;
    }

    private static long performDeleteBenchmark(DB db, List<Map<String, ByteIterator>> dataset, String dbName) throws Exception {
        System.out.println("\n*** Testing DELETE operation on " + dbName + " ***");

        long deleteStartTime = System.currentTimeMillis();

        for (int i = 0; i < dataset.size(); i++) {
            String key = KEY_PREFIX + "record" + i;
            db.delete("usertable", key);
        }

        long deleteEndTime = System.currentTimeMillis();
        long deleteTime = deleteEndTime - deleteStartTime;
        System.out.println("Delete time: " + deleteTime + " ms");
        return deleteTime;
    }

    private static long performCustomQuery(DB db, List<Map<String, ByteIterator>> dataset, String dbName) throws Exception {
        System.out.println("\n*** Testing CUSTOM QUERY on " + dbName + " ***");
        System.out.println("Query: Find records where 'http_reply_code' is 200 and 'bytes' > 5000");

        long queryStartTime = System.currentTimeMillis();
        List<String> matchingKeys = new ArrayList<>();

        for (int i = 0; i < dataset.size(); i++) {
            String key = KEY_PREFIX + "record" + i;
            HashMap<String, ByteIterator> result = new HashMap<>();
            db.read("usertable", key, FIELDS, result);

            Map<String, String> dataMap = deserializeFields(result);

            String replyCodeStr = dataMap.getOrDefault("http_reply_code", "0");
            String bytesStr = dataMap.getOrDefault("bytes", "0");

            int replyCode = (int) Double.parseDouble(replyCodeStr);
            int bytes = (int) Double.parseDouble(bytesStr);

            if (replyCode == 200 && bytes > 5000) {
                matchingKeys.add(key);
            }
        }

        long queryEndTime = System.currentTimeMillis();
        long queryTime = queryEndTime - queryStartTime;

        System.out.println("Custom query time: " + queryTime + " ms");
        System.out.println("Number of matching records: " + matchingKeys.size());

        return queryTime;
    }

    private static List<Map<String, ByteIterator>> loadDataset(int size) throws IOException {
        List<Map<String, ByteIterator>> dataset = new ArrayList<>();

        InputStream inputStream = BenchmarkTest.class.getClassLoader().getResourceAsStream("parsed_NASA_access_log.json");
        if (inputStream == null) {
            throw new FileNotFoundException("File not found: parsed_NASA_access_log.json");
        }

        // Read the JSON file into a map because the JSON file is an array of JSON objects
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        Gson gson = new Gson();
        // Use TypeToken to deserialize the JSON array to a list of maps of key-value pairs
        Map<String, Object>[] jsonArray = gson.fromJson(reader, Map[].class);
        reader.close();

        int actualSize = Math.min(size, jsonArray.length);

        for (int i = 0; i < actualSize; i++) {
            Map<String, Object> jsonMap = jsonArray[i];
            Map<String, ByteIterator> entry = new HashMap<>();

            for (Map.Entry<String, Object> jsonEntry : jsonMap.entrySet()) {
                // Convert all values to StringByteIterator because YCSB expects ByteIterator
                entry.put(jsonEntry.getKey(), new StringByteIterator(jsonEntry.getValue().toString()));
            }
            dataset.add(entry);
        }

        return dataset;
    }

    private static void clearRedis() {
        try {
            System.out.println("Clearing Redis database...");
            Process p = Runtime.getRuntime().exec("redis-cli FLUSHALL");
            p.waitFor();
            System.out.println("Redis cleared.");
        } catch (Exception e) {
            System.err.println("Error clearing Redis: " + e.getMessage());
        }
    }

    private static void clearMemcached() {
        try {
            System.out.println("Clearing Memcached database...");
            String[] cmd = { "sh", "-c", "echo flush_all | nc localhost 11211" };
            Process p = Runtime.getRuntime().exec(cmd);
            p.waitFor();
            System.out.println("Memcached cleared.");
        } catch (Exception e) {
            System.err.println("Error clearing Memcached: " + e.getMessage());
        }
    }

    /**
     * Deserialize fields from JSON in field0.
     */
    private static Map<String, String> deserializeFields(HashMap<String, ByteIterator> result) {
        Gson gson = new Gson();
        // Get the JSON string from field0
        ByteIterator field0 = result.get("field0");
        if (field0 == null) {
            return new HashMap<>();
        }
        // Convert the JSON string to a map because we serialized all fields into a single JSON object
        String jsonValue = field0.toString();
        // Use TypeToken to deserialize the JSON string to a map of key-value pairs
        Map<String, String> tempMap = gson.fromJson(jsonValue, new TypeToken<Map<String, String>>(){}.getType());
        return new HashMap<>(tempMap);
    }
}
