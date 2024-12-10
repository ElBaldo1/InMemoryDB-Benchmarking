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
 * It also performs a custom query at the end. After the benchmark is complete, it clears both Redis and Memcached databases.
 *
 * Key points:
 * - We serialize all fields of each record into a single JSON (stored in field0).
 * - We use a consistent key prefix (e.g., "ab-") so that keys match the naming convention used by Memcached.
 * - This approach ensures that the code works smoothly with both Redis and Memcached.
 */

public class BenchmarkTest {

    // Define the dataset sizes to be tested. We can use multiple sizes, here just one (1000).
    private static final int[] DATASET_SIZES = {1000,10000,100000};
    // Output CSV file for benchmark results
    private static final String OUTPUT_CSV_FILE = "output/benchmark_results.csv";

    // Original fields from the dataset
    private static final String[] FIELD_NAMES = {"host", "timestamp", "request", "http_reply_code", "bytes"};

    // We only store JSON in field0 for each record
    private static final Set<String> FIELDS = new HashSet<>(Collections.singletonList("field0"));

    // Define a prefix for keys so that both Redis and Memcached behave consistently.
    private static final String KEY_PREFIX = "ab-";

    public static void main(String[] args) throws Exception {
        System.out.println("Current Working Directory: " + System.getProperty("user.dir"));

        File outputDir = new File("output");
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }

        // Configure log4j for logging
        PropertyConfigurator.configure(BenchmarkTest.class.getClassLoader().getResource("log4j.properties"));
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

            Properties redisProps = initializeRedisProperties(size);
            runBenchmark(redisProps, size, "Redis", csvWriter);
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
        }

        System.out.println("\n===============================================");
        System.out.println("               Benchmark Complete");
        System.out.println("===============================================\n");

        csvWriter.close();

        // After all benchmarks are done, let's clear both Redis and Memcached.
        clearRedis();
        clearMemcached();
    }

    /**
     * Initialize properties for Redis.
     * This sets up a YCSB "database" configuration targeting the Redis binding.
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
     * This sets up a YCSB "database" configuration targeting the Memcached binding.
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
        // Set fieldcount for YCSB measurement
        props.setProperty("fieldcount", "5");
        Measurements.setProperties(props);

        Tracer tracer = new Builder("NoopTracer").build();
        DB db = DBFactory.newDB(props.getProperty("db"), props, tracer);

        db.init();

        // Load dataset from a JSON file (the NASA HTTP logs)
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
     * Use KEY_PREFIX + "record" + i as the key to ensure consistency for Memcached.
     */
    private static long performInsertionBenchmark(DB db, List<Map<String, ByteIterator>> dataset, String dbName) throws Exception {
        System.out.println("\n*** Testing INSERT operation on " + dbName + " ***");
        Gson gson = new Gson();
        long insertStartTime = System.currentTimeMillis();

        for (int i = 0; i < dataset.size(); i++) {
            String key = KEY_PREFIX + "record" + i;
            Map<String, ByteIterator> originalEntry = dataset.get(i);

            // Build a map of field-value pairs
            Map<String, String> dataMap = new HashMap<>();
            for (String fname : FIELD_NAMES) {
                ByteIterator val = originalEntry.get(fname);
                dataMap.put(fname, val != null ? val.toString() : "0");
            }

            // Serialize to JSON
            String jsonValue = gson.toJson(dataMap);
            Map<String, ByteIterator> ycsbEntry = new HashMap<>();
            ycsbEntry.put("field0", new StringByteIterator(jsonValue));

            // Insert into the DB
            db.insert("usertable", key, ycsbEntry);
        }

        long insertEndTime = System.currentTimeMillis();
        long insertionTime = insertEndTime - insertStartTime;
        System.out.println("Insertion time: " + insertionTime + " ms");
        return insertionTime;
    }

    /**
     * Deserialize the fields from the JSON stored in field0.
     */
    private static Map<String, String> deserializeFields(HashMap<String, ByteIterator> result) {
        Gson gson = new Gson();
        ByteIterator field0 = result.get("field0");
        if (field0 == null) {
            // If field0 is missing, return an empty, modifiable map
            return new HashMap<>();
        }
        String jsonValue = field0.toString();
        Map<String, String> tempMap = gson.fromJson(jsonValue, new TypeToken<Map<String, String>>(){}.getType());
        return new HashMap<>(tempMap);
    }

    /**
     * Perform READ benchmark: Retrieve each record by key and print the first two for debugging.
     */
    private static long performReadBenchmark(DB db, List<Map<String, ByteIterator>> dataset, String dbName) throws Exception {
        System.out.println("\n*** Testing READ operation on " + dbName + " ***");
        long readStartTime = System.currentTimeMillis();

        for (int i = 0; i < dataset.size(); i++) {
            String key = KEY_PREFIX + "record" + i;
            HashMap<String, ByteIterator> result = new HashMap<>();
            db.read("usertable", key, FIELDS, result);

            Map<String, String> dataMap = deserializeFields(result);
            if (i < 2) {
                System.out.println("Read key: " + key + ", data: " + dataMap);
            }
        }

        long readEndTime = System.currentTimeMillis();
        long readTime = readEndTime - readStartTime;
        System.out.println("Read time: " + readTime + " ms");
        return readTime;
    }

    /**
     * Perform UPDATE benchmark: Increase the 'bytes' field by 1000 for each record.
     */
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

    /**
     * Perform DELETE benchmark: Remove each record from the database.
     */
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

    /**
     * Perform a custom query: find all records where 'http_reply_code' is 200 and 'bytes' > 5000.
     * We simulate a "query" by scanning all keys and filtering client-side, since Memcached doesn't support queries.
     */
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

    /**
     * Load the dataset from a JSON file. Each entry is converted into a map of String->ByteIterator.
     */
    private static List<Map<String, ByteIterator>> loadDataset(int size) throws IOException {
        List<Map<String, ByteIterator>> dataset = new ArrayList<>();

        InputStream inputStream = BenchmarkTest.class.getClassLoader().getResourceAsStream("parsed_NASA_access_log.json");
        if (inputStream == null) {
            throw new FileNotFoundException("File not found: parsed_NASA_access_log.json");
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        com.google.gson.Gson gson = new com.google.gson.Gson();
        Map<String, Object>[] jsonArray = gson.fromJson(reader, Map[].class);
        reader.close();

        int actualSize = Math.min(size, jsonArray.length);

        for (int i = 0; i < actualSize; i++) {
            Map<String, Object> jsonMap = jsonArray[i];
            Map<String, ByteIterator> entry = new HashMap<>();

            for (Map.Entry<String, Object> jsonEntry : jsonMap.entrySet()) {
                entry.put(jsonEntry.getKey(), new StringByteIterator(jsonEntry.getValue().toString()));
            }
            dataset.add(entry);
        }

        return dataset;
    }

    /**
     * Clears the Redis database by running 'redis-cli FLUSHALL'.
     * This removes all keys from Redis.
     */
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

    /**
     * Clears the Memcached database by sending 'flush_all' command via netcat (nc).
     * This removes all keys from Memcached.
     */
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
}
