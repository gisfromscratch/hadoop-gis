package edu.hadoop.gis.samples.hbase;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Map;

/**
 * Imports text file directly into HBase using the client API.
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
	
	public void importInto(String tableName) throws IOException {
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
			
			while (null != (line = reader.readLine())) {
				String[] splittedLine = line.split(schema.getDelimiter());
				for (int tokenIndex = 0, tokenCount = splittedLine.length; tokenIndex < tokenCount; tokenIndex++) {
					if (fields.containsKey(tokenIndex)) {
						// Get the field using the token index
						TextFileField field = fields.get(tokenIndex);
						
					}
				}
			}
		} finally {
			reader.close();
		}
	}
}
