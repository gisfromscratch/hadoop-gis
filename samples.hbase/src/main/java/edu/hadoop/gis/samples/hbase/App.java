package edu.hadoop.gis.samples.hbase;

import java.io.File;

/**
 * Simple HBase client for importing text files into HBase tables.
 * 
 * @author Jan Tschada
 *
 */
public class App {

	public static void main(String[] args) {
		File inputFile = null;
		String tableName = null;
		boolean readKey = true;
		AppTypeArgument argumentType = AppTypeArgument.Unknown;
		for (String arg : args) {
			if (readKey) {
				if ("-CSV".equalsIgnoreCase(arg)) {
					argumentType = AppTypeArgument.InputFile;
				} else if ("-TABLE".equalsIgnoreCase(arg)) {
					argumentType = AppTypeArgument.TableName;
				}
			} else {
				switch (argumentType) {
				case InputFile:
					inputFile = new File(arg);
					break;
				case TableName:
					tableName = arg;
					break;
				}
			}
			readKey = !readKey;
		}
		
		if (null == inputFile || null == tableName) {
			System.err.println("Usage: -csv <input_file> -table <hbase-table>");
			return;
		}
		
		
	}

	/**
	 * The argument types of this client app.
	 * 
	 * @author Jan Tschada
	 *
	 */
	private enum AppTypeArgument {
		Unknown, InputFile, TableName
	}
}
