# 🚀 In-Memory Database Benchmarking

## Project Overview

This project benchmarks the performance of two in-memory database technologies, **Redis** and **Memcached**, using datasets of varying sizes. The benchmarking focuses on two key operations:

1. **Insertions**: Adding dataset entries into the database.  
2. **Reads**: Retrieving dataset entries from the database.  

The benchmarking results provide insights into the **performance** and **scalability** of Redis and Memcached when handling datasets of different sizes.

---

## Table of Contents
1. [Prerequisites](#prerequisites)
2. [Setup Instructions](#setup-instructions)
3. [Running the Benchmark](#running-the-benchmark)
4. [Rebuilding the JAR](#rebuilding-the-jar)
5. [Results and Output](#results-and-output)
6. [Error Handling](#error-handling)
7. [Additional Notes](#additional-notes)
8. [License](#license)

---

## Prerequisites

To set up and run the project, ensure you have the following:
- **Java 8** or a newer version installed.
- At least **4GB of RAM** available.
- **YCSB** (Yahoo! Cloud Serving Benchmark) tools installed.
- Local installations of **Redis** and **Memcached**:
  - Redis runs on port `6379`.
  - Memcached runs on port `11211`.

---

## Setup Instructions

### 1. Clone and Install YCSB Tools

Clone the YCSB repository and install dependencies:

```bash
git clone https://github.com/brianfrankcooper/YCSB.git
cd YCSB
mvn clean package
```

Install Redis and Memcached bindings:

```bash
mvn -pl site.ycsb:redis-binding -am clean package
mvn -pl site.ycsb:memcached-binding -am clean package
```

### 2. Start Redis and Memcached Servers

Run Redis and Memcached in separate terminal windows:

```bash
redis-server
memcached -p 11211 -u nobody -d
```

---

## Running the Benchmark

### 1. Run the Pre-Built JAR

Execute the benchmarking tests using the provided pre-built JAR file:

```bash
java -Xmx4g -jar out/artifacts/memdbJava_jar/memdbJava.jar
```

### 2. Generate Output Folder

To store the benchmarking results, create an `output` folder in the same directory as `BenchmarkTest.java`:

```bash
mkdir output
```

---

## Rebuilding the JAR

### Step-by-Step Instructions:

1. **Clean Existing Builds**:
   ```bash
   mvn clean
   ```

2. **Build the JAR**:
   ```bash
   mvn package
   ```

3. **Verify the Rebuilt JAR**:
   Check if the required classes (e.g., `MemcachedClient`) exist:
   ```bash
   jar tf target/memdbJava-1.0-SNAPSHOT.jar | grep MemcachedClient
   ```

4. **Run the New JAR**:
   ```bash
   java -Xmx4g -jar target/memdbJava-1.0-SNAPSHOT.jar
   ```

---

## Results and Output

The benchmarking results will be saved as a CSV file in the `output` folder, for example:

- `output/benchmark_results.csv`

This file contains performance metrics such as:
- Insert and read operation times.
- Latency for Redis and Memcached.

---

## Error Handling

### NullPointerException Handling

- Some records may not contain the keys `http_reply_code` or `bytes`.  
- To handle missing values:
  - **Default Values** are assigned:  
    - `http_reply_code`: `"0"`  
    - `bytes`: `"0.0"`

- If parsing fails, default numerical values are used to prevent crashes.

---

## Additional Notes

- All project comments and custom logic details can be found directly in the source files.  
- Key file: `BenchmarkTest.java`

