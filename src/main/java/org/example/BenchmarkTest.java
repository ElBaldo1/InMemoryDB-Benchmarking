package org.example;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBFactory;
import site.ycsb.StringByteIterator;
import site.ycsb.measurements.Measurements;
import org.apache.htrace.core.Tracer;
import org.apache.htrace.core.Tracer.Builder;

import java.util.*;
import java.io.*;

public class BenchmarkTest {
    public static void main(String[] args) throws Exception {
        // Dataset sizes to test
        int[] datasetSizes = {1000, 10000, 100000, 1000000};

        // Run benchmarks for Redis
        System.out.println("Starting benchmark for Redis");
        for (int size : datasetSizes) {
            System.out.println("Test with dataset size: " + size);

            // Initialize properties for Redis
            Properties redisProps = new Properties();
            redisProps.setProperty("db", "site.ycsb.db.RedisClient");
            redisProps.setProperty("redis.host", "localhost");
            redisProps.setProperty("redis.port", "6379");
            // Set required properties
            redisProps.setProperty("recordcount", String.valueOf(size));
            redisProps.setProperty("operationcount", String.valueOf(size));
            redisProps.setProperty("workload", "site.ycsb.workloads.CoreWorkload");

            runBenchmark(redisProps, size);
        }

        // Run benchmarks for Memcached
        System.out.println("Starting benchmark for Memcached");
        for (int size : datasetSizes) {
            System.out.println("Test with dataset size: " + size);

            // Initialize properties for Memcached
            Properties memcachedProps = new Properties();
            memcachedProps.setProperty("db", "site.ycsb.db.MemcachedCompatibleClient");
            memcachedProps.setProperty("memcached.hosts", "localhost:11211");
            // Set required properties
            memcachedProps.setProperty("recordcount", String.valueOf(size));
            memcachedProps.setProperty("operationcount", String.valueOf(size));
            memcachedProps.setProperty("workload", "site.ycsb.workloads.CoreWorkload");

            runBenchmark(memcachedProps, size);
        }
    }

    private static void runBenchmark(Properties props, int datasetSize) throws Exception {
        // Initialize Measurements with properties before any other operation
        Measurements.setProperties(props);

        // Initialize the database with a default tracer
        Tracer tracer = new Builder("NoopTracer").build();
        DB db = DBFactory.newDB(props.getProperty("db"), props, tracer);
        db.init();

        // Load dataset
        List<Map<String, ByteIterator>> dataset = loadDataset(datasetSize);

        // Measure insertion time
        long insertStartTime = System.currentTimeMillis();
        for (int i = 0; i < dataset.size(); i++) {
            String key = "user" + i;
            db.insert("usertable", key, dataset.get(i));
        }
        long insertEndTime = System.currentTimeMillis();
        System.out.println("Insertion time: " + (insertEndTime - insertStartTime) + " ms");

        // Measure read time
        long readStartTime = System.currentTimeMillis();
        for (int i = 0; i < dataset.size(); i++) {
            String key = "user" + i;
            Set<String> fields = null;
            HashMap<String, ByteIterator> result = new HashMap<>();
            db.read("usertable", key, fields, result);
        }
        long readEndTime = System.currentTimeMillis();
        System.out.println("Read time: " + (readEndTime - readStartTime) + " ms");

        db.cleanup();
    }

    private static List<Map<String, ByteIterator>> loadDataset(int size) throws IOException {
        List<Map<String, ByteIterator>> dataset = new ArrayList<>();
        InputStream inputStream = BenchmarkTest.class.getClassLoader().getResourceAsStream("parsed_NASA_access_log.json");
        if (inputStream == null) {
            throw new FileNotFoundException("File not found: parsed_NASA_access_log.json");
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        com.google.gson.Gson gson = new com.google.gson.Gson();

        Map<String, Object>[] jsonArray = gson.fromJson(reader, Map[].class);
        // Limit the dataset to the requested size
        for (int i = 0; i < Math.min(size, jsonArray.length); i++) {
            Map<String, Object> jsonMap = jsonArray[i];
            Map<String, ByteIterator> entry = new HashMap<>();
            // Convert each JSON entry to a Map<String, ByteIterator>
            for (Map.Entry<String, Object> jsonEntry : jsonMap.entrySet()) {
                entry.put(jsonEntry.getKey(), new StringByteIterator(jsonEntry.getValue().toString()));
            }
            dataset.add(entry);
        }
        reader.close();
        return dataset;
    }
}
