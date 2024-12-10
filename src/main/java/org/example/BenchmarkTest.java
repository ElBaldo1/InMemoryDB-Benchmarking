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
 * It also performs a custom query at the end. After completion, it clears both Redis and Memcached databases.
 */
public class BenchmarkTest {

    private static final int[] DATASET_SIZES = {1000};
    private static final String OUTPUT_CSV_FILE = "output/benchmark_results.csv";
    // Campi originali
    private static final String[] FIELD_NAMES = {"host", "timestamp", "request", "http_reply_code", "bytes"};
    // Leggiamo sempre e solo "field0" che contiene il JSON
    private static final Set<String> FIELDS = new HashSet<>(Collections.singletonList("field0"));

    public static void main(String[] args) throws Exception {
        System.out.println("Current Working Directory: " + System.getProperty("user.dir"));

        File outputDir = new File("output");
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }

        PropertyConfigurator.configure(BenchmarkTest.class.getClassLoader().getResource("log4j.properties"));
        Logger.getLogger("net.spy.memcached").setLevel(org.apache.log4j.Level.ERROR);

        CSVWriter csvWriter = new CSVWriter(OUTPUT_CSV_FILE);
        csvWriter.writeHeader("Database", "Dataset Size", "Operation", "Time (ms)");

        // Redis Benchmark
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

        // Memcached Benchmark
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
    }

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

    private static Properties initializeMemcachedProperties(int size) {
        Properties props = new Properties();
        props.setProperty("db", "site.ycsb.db.MemcachedClient");
        props.setProperty("memcached.hosts", "localhost:11211");
        props.setProperty("recordcount", String.valueOf(size));
        props.setProperty("operationcount", String.valueOf(size));
        props.setProperty("workload", "site.ycsb.workloads.CoreWorkload");
        return props;
    }

    private static void runBenchmark(Properties props, int datasetSize, String dbName, CSVWriter csvWriter) throws Exception {
        props.setProperty("fieldcount", "5");
        Measurements.setProperties(props);

        Tracer tracer = new Builder("NoopTracer").build();
        DB db = DBFactory.newDB(props.getProperty("db"), props, tracer);

        db.init();

        List<Map<String, ByteIterator>> dataset = loadDataset(datasetSize);

        // INSERT benchmark
        long insertTime = performInsertionBenchmark(db, dataset, dbName);
        csvWriter.writeRecord(dbName, String.valueOf(datasetSize), "INSERT", String.valueOf(insertTime));

        // READ benchmark
        long readTime = performReadBenchmark(db, dataset, dbName);
        csvWriter.writeRecord(dbName, String.valueOf(datasetSize), "READ", String.valueOf(readTime));

        // UPDATE benchmark
        long updateTime = performUpdateBenchmark(db, dataset, dbName);
        csvWriter.writeRecord(dbName, String.valueOf(datasetSize), "UPDATE", String.valueOf(updateTime));

        // CUSTOM QUERY benchmark
        long customQueryTime = performCustomQuery(db, dataset, dbName);
        csvWriter.writeRecord(dbName, String.valueOf(datasetSize), "CUSTOM_QUERY", String.valueOf(customQueryTime));

        // DELETE benchmark
        long deleteTime = performDeleteBenchmark(db, dataset, dbName);
        csvWriter.writeRecord(dbName, String.valueOf(datasetSize), "DELETE", String.valueOf(deleteTime));

        db.cleanup();
    }

    // Inserisce tutti i campi in un JSON unico in field0
    private static long performInsertionBenchmark(DB db, List<Map<String, ByteIterator>> dataset, String dbName) throws Exception {
        System.out.println("\n*** Testing INSERT operation on " + dbName + " ***");
        Gson gson = new Gson();
        long insertStartTime = System.currentTimeMillis();

        for (int i = 0; i < dataset.size(); i++) {
            String key = "record" + i;
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

    // Deserializza i campi da field0
    private static Map<String, String> deserializeFields(HashMap<String, ByteIterator> result) {
        Gson gson = new Gson();
        ByteIterator field0 = result.get("field0");
        if (field0 == null) {
            // Niente field0, ritorno una mappa vuota ma modificabile
            return new HashMap<>();
        }
        String jsonValue = field0.toString();
        Map<String, String> tempMap = gson.fromJson(jsonValue, new TypeToken<Map<String, String>>(){}.getType());
        // Creo una nuova HashMap modificabile
        return new HashMap<>(tempMap);
    }

    private static long performReadBenchmark(DB db, List<Map<String, ByteIterator>> dataset, String dbName) throws Exception {
        System.out.println("\n*** Testing READ operation on " + dbName + " ***");
        long readStartTime = System.currentTimeMillis();

        for (int i = 0; i < dataset.size(); i++) {
            String key = "record" + i;
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

    private static long performUpdateBenchmark(DB db, List<Map<String, ByteIterator>> dataset, String dbName) throws Exception {
        System.out.println("\n*** Testing UPDATE operation on " + dbName + " ***");
        Gson gson = new Gson();
        long updateStartTime = System.currentTimeMillis();

        for (int i = 0; i < dataset.size(); i++) {
            String key = "record" + i;
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
            String key = "record" + i;
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
            String key = "record" + i;
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
}
