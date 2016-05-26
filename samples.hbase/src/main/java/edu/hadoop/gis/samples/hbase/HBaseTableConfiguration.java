package edu.hadoop.gis.samples.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * Represents a HBase table configuration.
 * @author Jan Tschada
 *
 */
public class HBaseTableConfiguration {

	private final String hosts;
	
	private final String zookeeperParentNode;
	
	private final Configuration configuration;
	
	private final String tableName;

	public HBaseTableConfiguration(String hosts, String zookeeperParentNode, String tableName) {
		this.hosts = hosts;
		this.zookeeperParentNode = zookeeperParentNode;
		
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
}
