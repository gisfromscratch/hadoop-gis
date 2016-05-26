package edu.hadoop.gis.samples.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * Represents a HBase table configuration.
 * @author Jan Tschada
 *
 */
public class HBaseTableConfiguration {

	private final Configuration configuration;
	
	private final String tableName;

	/**
	 * Creates a new table configuration.
	 * @param hosts the HBase hosts comma separated. e.g. hbase01.com,hbase02.com
	 * @param zookeeperParentNode the zookeeper parent node. e.g. /hbase-secure
	 * @param tableName the name of the HBase table.
	 */
	public HBaseTableConfiguration(String hosts, String zookeeperParentNode, String tableName) {
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum", hosts);
		configuration.set("zookeeper.znode.parent", zookeeperParentNode);
		this.tableName = tableName;
	}
	
	public Configuration getConfiguration() {
		return configuration;
	}

	public String getTableName() {
		return tableName;
	}
	
	/**
	 * Creates a new table configuration using the environment variables <code>hbase.hosts</code>
	 * and <code>hbase.zookeeper.node</code>.
	 * @param tableName the name of the HBase table.
	 * @return A new table configuration.
	 */
	public static HBaseTableConfiguration createFromEnvironment(String tableName) {
		final String hostsKey = "hbase.hosts";
		final String zookeeperKey = "hbase.zookeeper.node";
		String hostsValue = System.getenv(hostsKey);
		if (null == hostsValue) {
			throw new IllegalStateException(String.format("Environment variable %s is not set!", hostsKey));
		}
		String zookeeperValue = System.getenv(zookeeperKey);
		if (null == zookeeperValue) {
			throw new IllegalStateException(String.format("Environment variable %s is not set!", zookeeperKey));
		}
		
		return new HBaseTableConfiguration(hostsValue, zookeeperValue, tableName);
	}
}
