# In-Memory Database Benchmarking

## Project Overview
This project benchmarks the performance of two in-memory database technologies, **Redis** and **Memcached**, using datasets of varying sizes. The benchmarking process involves testing two operations:
1. **Insertions:** Adding dataset entries into the database.
2. **Reads:** Retrieving dataset entries from the database.

The benchmark results provide insights into the performance and scalability of Redis and Memcached for different dataset sizes.

---

## How to Run the Project

### Prerequisites
1. Ensure **Java 8** or a newer version is installed on your system.
2. Make sure you have at least **4GB of available RAM** for the benchmarking process.
3. Redis and Memcached servers should be running locally:
   - Redis on port `6379`
   - Memcached on port `11211`

### Command to Run the Tests
To execute the benchmarking tests, use the following command:

```bash
java -Xmx4g -jar out/artifacts/memdbJava_jar/memdbJava.jar
```
## Steps to Rebuild the JAR
### 1. Clean the Maven Build Directory
   To clean the existing build artifacts, run the following command:
```bash
mvn clean
```

### 2. Build the JAR
To rebuild the JAR after making changes to the code, execute the following Maven command:
```bash
mvn package
```

### 3. Verify the Rebuilt JAR
Ensure that the rebuilt JAR contains the required classes (e.g., MemcachedClient) by running:

```bash
jar tf target/memdbJava-1.0-SNAPSHOT.jar | grep MemcachedClient
```

### 4. Run the Application
   To execute the benchmarking application using the newly built JAR, use the command below:
```bash
java -Xmx4g -jar target/memdbJava-1.0-SNAPSHOT.jar
```

# For more details check the comments in the files