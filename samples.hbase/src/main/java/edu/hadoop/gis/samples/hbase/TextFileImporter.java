package edu.hadoop.gis.samples.hbase;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Imports text file directly into HBase using the client API.
 * 
 * @author Jan Tschada
 *
 */
public class TextFileImporter {

	private final Logger logger;

	private final File inputFile;

	private final TextFileSchema schema;

	/**
	 * Creates a new importer.
	 * 
	 * @param inputFile
	 *            the file which should be imported.
	 * @param schema
	 *            the schema of the file specified.
	 */
	public TextFileImporter(File inputFile, TextFileSchema schema) {
		if (null == inputFile) {
			throw new IllegalArgumentException("The file must not be null!");
		}
		if (!inputFile.canRead()) {
			throw new IllegalArgumentException("The file cannot be read!");
		}
		if (null == schema) {
			throw new IllegalArgumentException("The schema must not be null!");
		}

		this.logger = LogManager.getLogger(getClass());
		this.inputFile = inputFile;
		this.schema = schema;
	}

	public void importInto(HBaseTableImportConfiguration importConfiguration) throws IOException, InterruptedException {
		if (null == importConfiguration) {
			throw new IllegalArgumentException("The configuration must not be null!");
		}

		LineNumberReader reader = new LineNumberReader(new FileReader(inputFile));
		try {
			String line;
			Map<Integer, TextFileField> fields = schema.getFields();
			if (schema.hasHeader()) {
				// Omit the first line
				if (null == (line = reader.readLine())) {
					// End of the stream was reached
					return;
				}

				// No fields are set
				if (fields.isEmpty()) {
					String[] splittedLine = line.split(schema.getDelimiter());
					for (int tokenIndex = 0, tokenCount = splittedLine.length; tokenIndex < tokenCount; tokenIndex++) {
						String fieldName = splittedLine[tokenIndex];
						TextFileField field = new TextFileField(tokenIndex, fieldName);
						fields.put(tokenIndex, field);
					}
				}
			}

			// Put data into the table
			HBaseTableConfiguration tableConfiguration = importConfiguration.getConfiguration();
			HTable table = new HTable(tableConfiguration.getConfiguration(), tableConfiguration.getTableName());
			try {
				String columnFamily = importConfiguration.getColumnFamily();
				byte[] columnFamilyAsBytes = Bytes.toBytes(columnFamily);

				// Doing bulk inserts
				int autoCommitSize = importConfiguration.getAutoCommitSize();
				if (0 < autoCommitSize) {
					autoCommitSize = 1;
				}
				List<Put> puts = new ArrayList<Put>(autoCommitSize);
				Object[] results = new Object[autoCommitSize];

				while (null != (line = reader.readLine())) {
					// Create the put using the line number
					Put recordPut = new Put(Bytes.toBytes(reader.getLineNumber()));

					String[] splittedLine = line.split(schema.getDelimiter());
					for (int tokenIndex = 0, tokenCount = splittedLine.length; tokenIndex < tokenCount; tokenIndex++) {
						if (fields.containsKey(tokenIndex)) {
							// Get the field using the token index
							TextFileField field = fields.get(tokenIndex);
							String value = splittedLine[tokenIndex];

							// Put the next value
							recordPut.add(columnFamilyAsBytes, Bytes.toBytes(field.getName()), Bytes.toBytes(value));
						}
					}

					puts.add(recordPut);
					if (puts.size() == autoCommitSize) {
						// Put the records into the table
						batchInsert(table, puts, results);
					}
				}
				
				// Check if there are still uncommitted puts available
				if (!puts.isEmpty()) {
					// Put the records into the table
					batchInsert(table, puts, results);
				}
			} finally {
				table.close();
			}
		} finally {
			reader.close();
		}
	}

	private void batchInsert(HTable table, List<Put> puts, Object[] results) throws IOException, InterruptedException {
		try {
			table.batch(puts, results);
			puts.clear();
		} catch (InterruptedException e) {
			logger.error("Batch insert failed!", e);
			throw e;
		}
	}
}
