package edu.hadoop.gis.samples.hbase;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents the schema of a text file.
 * 
 * @author Jan Tschada
 *
 */
public class TextFileSchema {

	private boolean header;

	private String delimiter;

	private Map<Integer, TextFileField> fields;

	public TextFileSchema(boolean header, String delimiter) {
		this.header = header;
		this.delimiter = delimiter;
		this.fields = new HashMap<Integer, TextFileField>();
	}

	public Map<Integer, TextFileField> getFields() {
		return fields;
	}

	public void setFields(Map<Integer, TextFileField> fields) {
		if (null == fields) {
			throw new IllegalArgumentException("The fields must not be null!");
		}

		this.fields = fields;
	}

	public boolean hasHeader() {
		return header;
	}

	public void setHeader(boolean header) {
		this.header = header;
	}

	public String getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}
}
