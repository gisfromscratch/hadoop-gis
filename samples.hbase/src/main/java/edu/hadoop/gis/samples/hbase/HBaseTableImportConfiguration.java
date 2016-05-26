package edu.hadoop.gis.samples.hbase;

/**
 * Represents the configuration for importing data into an HBase table.
 * @author Jan Tschada
 *
 */
public class HBaseTableImportConfiguration {

	private final HBaseTableConfiguration configuration;
	
	private final String columnFamily;
	
	private final int autoCommitSize;

	/**
	 * Creates a new configuration instance.
	 * @param configuration the configuration of the HBase table.
	 * @param columnFamily the name of the column family which should be used for importing the data.
	 * @param autoCommitSize the number of records for doing bulk inserts.
	 */
	public HBaseTableImportConfiguration(HBaseTableConfiguration configuration, String columnFamily, int autoCommitSize) {
		if (null == configuration) {
			throw new IllegalArgumentException("The configuration must not be null!");
		}
		if (null == columnFamily) {
			throw new IllegalArgumentException("The column family must not be null!");
		}
		if (autoCommitSize < 1) {
			throw new IllegalArgumentException("The auto commit size must be greater than 0!");
		}
		
		this.columnFamily = columnFamily;
		this.autoCommitSize = autoCommitSize;
		this.configuration = configuration;
	}

	public HBaseTableConfiguration getConfiguration() {
		return configuration;
	}
	
	public String getColumnFamily() {
		return columnFamily;
	}

	public int getAutoCommitSize() {
		return autoCommitSize;
	}
}
