package edu.hadoop.gis.samples.hbase;

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

/**
 * Tests for importing text files into HBase.
 * @author Jan Tschada
 *
 */
public class ImportTextFileTest extends TestCase {

	public void testFileImport() throws IOException, InterruptedException {
		TextFileSchema schema = new TextFileSchema(true, "\t");
		File inputFile = new File(getClass().getClassLoader().getResource("aa.txt").getFile());
		TextFileImporter importer = new TextFileImporter(inputFile, schema);
		HBaseTableConfiguration tableConfiguration = HBaseTableConfiguration.createFromEnvironment("geonames");
		final int autoCommitSize = 1000;
		HBaseTableImportConfiguration importConfiguration = new HBaseTableImportConfiguration(tableConfiguration, "fields", autoCommitSize);
		importer.importInto(importConfiguration);
	}
}
