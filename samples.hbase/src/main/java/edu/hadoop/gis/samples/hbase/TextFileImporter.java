package edu.hadoop.gis.samples.hbase;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Imports text file directly into HBase using the client API.
 * 
 * @author Jan Tschada
 *
 */
public class TextFileImporter {

	private final File inputFile;
	private final TextFileSchema schema;

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

		this.inputFile = inputFile;
		this.schema = schema;
	}

	public void importInto(HBaseTableConfiguration tableConfiguration, String columnFamily) throws IOException {
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
			HTable table = new HTable(tableConfiguration.getConfiguration(), tableConfiguration.getTableName());
			try {
				byte[] columnFamilyAsBytes = Bytes.toBytes(columnFamily);
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
					
					// Put the record into the table
					table.put(recordPut);
				}
			} finally {
				table.close();
			}
		} finally {
			reader.close();
		}
	}
}
