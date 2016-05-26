package edu.hadoop.gis.samples.hbase;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Simple HBase client for importing text files into HBase tables.
 * 
 * @author Jan Tschada
 *
 */
public class App {
	
	private static Logger logger;
	
	static {
		logger = LogManager.getLogger(App.class);
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		File inputFile = null;
		String tableName = null;
		String columnFamily = null;
		boolean header = false;
		String separator = ",";
		boolean readKey = true;
		AppArgumentType argumentType = AppArgumentType.Unknown;
		for (String arg : args) {
			if (readKey) {
				if ("-CSV".equalsIgnoreCase(arg)) {
					argumentType = AppArgumentType.InputFile;
				} else if ("-TABLE".equalsIgnoreCase(arg)) {
					argumentType = AppArgumentType.TableName;
				} else if ("-CF".equalsIgnoreCase(arg)) {
					argumentType = AppArgumentType.ColumnFamily;
				} else if ("-HEADER".equalsIgnoreCase(arg)) {
					argumentType = AppArgumentType.Header;
					header = true;
					readKey = true;
					continue;
				} else if ("-SEP".equalsIgnoreCase(arg)) {
					argumentType = AppArgumentType.Separator;
				} else {
					argumentType = AppArgumentType.Unknown;
				}
			} else {
				switch (argumentType) {
				case InputFile:
					inputFile = new File(arg);
					break;
				case TableName:
					tableName = arg;
					break;
				case ColumnFamily:
					columnFamily = arg;
					break;
				case Separator:
					separator = arg;
					break;
				default:
					logger.warn("Unknown application argument!");
					break;
				}
			}
			readKey = !readKey;
		}
		
		if (null == inputFile || null == tableName) {
			logger.error("Usage: -csv <input_file> -table <hbase-table> -cf <hbase_column_family>");
			logger.error("Optional: -header, -sep <field_separator>");
			return;
		}
		
		try {
			// Create the configuration and the importer
			HBaseTableConfiguration tableConfiguration = HBaseTableConfiguration.createFromEnvironment(tableName);
			TextFileSchema schema = new TextFileSchema(header, separator);
			TextFileImporter importer = new TextFileImporter(inputFile, schema);
			
			// Create the import configuration
			final int autoCommitSize = 1000;
			HBaseTableImportConfiguration importConfiguration = new HBaseTableImportConfiguration(tableConfiguration, columnFamily, autoCommitSize);
			
			if (logger.isInfoEnabled()) {
				logger.info(String.format("Start importing %s", inputFile.getAbsolutePath()));
			}
			
			// Import the file
			importer.importInto(importConfiguration);
		} catch (Exception e) {
			logger.error(String.format("Importing %s into %s failed!", inputFile.getAbsolutePath(), tableName), e);
			return;
		}
		
		if (logger.isInfoEnabled()) {
			logger.info(String.format("%s was successfully imported into %s", inputFile.getAbsolutePath(), tableName));
		}
	}

	/**
	 * The argument types of this client app.
	 * 
	 * @author Jan Tschada
	 *
	 */
	private enum AppArgumentType {
		Unknown, InputFile, TableName, Header, Separator, ColumnFamily, AutoCommitSize
	}
}
