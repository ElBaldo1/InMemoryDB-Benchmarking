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
4. Load the data in the two databases by running the two scripts in the src/main/Python folder in the following order:
   - `converterData.py` to convert the data into kay-value.
   - `uploadFileDB.py` to load the data into the db.
### Be sure that the file with the data that you want to load is in the same directory as the scripts, ex: https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html
5. Redis and Memcached servers should be running locally:
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
### 5. Run the 2 db
   Run the following command to start the Redis and Memcached servers:
```bash
memcached -p 11211 -u nobody -d
redis-server
```


### 5. Run the Application
   To execute the benchmarking application using the newly built JAR, use the command below:
```bash
java -Xmx4g -jar target/memdbJava-1.0-SNAPSHOT.jar
```
### To see the results of the benchmarking, create the folder 'output' in the directory of the benchmarkTest.java file
# For more details check the comments in the files

### The result map in custom query does not contain the key "http_reply_code" or "bytes" for some records, so result.get("http_reply_code") or result.get("bytes") returns null, leading to the NullPointerException when calling toString() on null.
### We can choose to skip these records, assign default values, or log a warning. I have chosen to assign default values to avoid the NullPointerException.
For both 'http_reply_code' and 'bytes' fields, if they are missing (replyCodeBI == null or bytesBI == null), we assign default values of "0" and "0.0", respectively.
If parsing fails, we assign default numerical values to replyCode and bytes.