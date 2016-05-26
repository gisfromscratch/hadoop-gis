package edu.hadoop.gis.samples.hbase;

import junit.framework.TestCase;

public class HBaseConfigurationTest extends TestCase {

	public void testConfiguration() {
		HBaseTableConfiguration configuration = HBaseTableConfiguration.createFromEnvironment("geonames");
		assertNotNull("The configuration must not be null!", configuration);
	}
}
