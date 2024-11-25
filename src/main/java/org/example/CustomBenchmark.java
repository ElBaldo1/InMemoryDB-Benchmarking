package org.example;

import redis.clients.jedis.Jedis;
import net.spy.memcached.MemcachedClient;

import java.net.InetSocketAddress;
import java.util.*;
import java.io.*;

public class CustomBenchmark {
    public static void main(String[] args) throws Exception {
        int[] datasetSizes = {1000, 10000, 100000, 1000000};

        // Run tests for Redis
        System.out.println("Custom tests for Redis");
        for (int size : datasetSizes) {
            System.out.println("Test with dataset size: " + size);
            testRedis(size);
        }

        // Run tests for Memcached
        System.out.println("Custom tests for Memcached");
        for (int size : datasetSizes) {
            System.out.println("Test with dataset size: " + size);
            testMemcached(size);
        }
    }

    private static void testRedis(int datasetSize) throws Exception {
        // Initialize Redis client
        Jedis jedis = new Jedis("localhost", 6379);

        // Load dataset
        List<Map<String, String>> dataset = loadDatasetCustom(datasetSize);

        // Insertion
        long insertStart = System.currentTimeMillis();
        int i = 0;
        for (Map<String, String> entry : dataset) {
            String key = "user" + i++;
            jedis.hmset(key, entry);
        }
        long insertEnd = System.currentTimeMillis();
        System.out.println("Redis insertion time: " + (insertEnd - insertStart) + " ms");

        // Reading
        long readStart = System.currentTimeMillis();
        for (int j = 0; j < datasetSize; j++) {
            String key = "user" + j;
            jedis.hgetAll(key);
        }
        long readEnd = System.currentTimeMillis();
        System.out.println("Redis read time: " + (readEnd - readStart) + " ms");

        // Close the Redis client
        jedis.close();
    }

    private static void testMemcached(int datasetSize) throws Exception {
        // Initialize Memcached client
        MemcachedClient memcachedClient = new MemcachedClient(new InetSocketAddress("localhost", 11211));

        // Load dataset
        List<Map<String, String>> dataset = loadDatasetCustom(datasetSize);

        // Insertion
        long insertStart = System.currentTimeMillis();
        int i = 0;
        for (Map<String, String> entry : dataset) {
            String key = "user" + i++;
            memcachedClient.set(key, 3600, entry);
        }
        long insertEnd = System.currentTimeMillis();
        System.out.println("Memcached insertion time: " + (insertEnd - insertStart) + " ms");

        // Reading
        long readStart = System.currentTimeMillis();
        for (int j = 0; j < datasetSize; j++) {
            String key = "user" + j;
            memcachedClient.get(key);
        }
        long readEnd = System.currentTimeMillis();
        System.out.println("Memcached read time: " + (readEnd - readStart) + " ms");

        // Shut down the Memcached client
        memcachedClient.shutdown();
    }

    private static List<Map<String, String>> loadDatasetCustom(int size) throws IOException {
        List<Map<String, String>> dataset = new ArrayList<>();
        InputStream inputStream = CustomBenchmark.class.getClassLoader().getResourceAsStream("parsed_NASA_access_log.json");
        if (inputStream == null) {
            throw new FileNotFoundException("File not found: parsed_NASA_access_log.json");
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        com.google.gson.Gson gson = new com.google.gson.Gson();

        Map<String, Object>[] jsonArray = gson.fromJson(reader, Map[].class);
        // Limit the dataset to the requested size
        for (int i = 0; i < Math.min(size, jsonArray.length); i++) {
            Map<String, Object> jsonMap = jsonArray[i];
            Map<String, String> entry = new HashMap<>();
            // Convert each JSON entry to a Map<String, String>
            for (Map.Entry<String, Object> jsonEntry : jsonMap.entrySet()) {
                entry.put(jsonEntry.getKey(), jsonEntry.getValue().toString());
            }
            dataset.add(entry);
        }
        reader.close();
        return dataset;
    }
}
