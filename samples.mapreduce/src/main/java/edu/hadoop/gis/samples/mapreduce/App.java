package edu.hadoop.gis.samples.mapreduce;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Sample application using the File System API
 * 
 * @author Jan Tschada
 *
 */
public class App {
	
	private static final Log logger = LogFactory.getLog(App.class);
	
	public static void main(String[] args) {
		logger.info("Reading HDFS content...");
		
		Path rootPath = new Path("/");
		boolean isRoot = rootPath.isRoot();
		logger.info(String.format("%s root: %b", rootPath.getName(), isRoot));
	}
}
