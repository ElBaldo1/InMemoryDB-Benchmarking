package org.example;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * CSVWriter is a utility class for writing benchmark results to a CSV file.
 */
public class CSVWriter {
    private final PrintWriter writer;

    /**
     * Constructs a CSVWriter with the specified file path.
     *
     * @param filePath The path to the CSV file.
     * @throws IOException If an I/O error occurs.
     */
    public CSVWriter(String filePath) throws IOException {
        writer = new PrintWriter(new FileWriter(filePath, false)); // 'false' to overwrite existing file
    }

    /**
     * Writes the header row to the CSV file.
     *
     * @param headers The column headers.
     */
    public void writeHeader(String... headers) {
        writer.println(String.join(",", headers));
        writer.flush();
    }

    /**
     * Writes a single record to the CSV file.
     *
     * @param fields The fields of the record.
     */
    public void writeRecord(String... fields) {
        writer.println(String.join(",", fields));
        writer.flush();
    }

    /**
     * Closes the CSVWriter, releasing any system resources associated with it.
     */
    public void close() {
        if (writer != null) {
            writer.close();
        }
    }
}
