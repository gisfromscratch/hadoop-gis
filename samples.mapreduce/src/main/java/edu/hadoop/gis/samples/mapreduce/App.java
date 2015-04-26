package edu.hadoop.gis.samples.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.Job;

/**
 * Sample application using the File System API
 * 
 * @author Jan Tschada
 *
 */
public class App {
	
	private static final Log logger = LogFactory.getLog(App.class);
	
	public static void main(String[] args) throws IOException {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		FileSystem fileSystem = FileSystem.get(configuration);
		logger.info("Reading HDFS content...");
		
		Path rootPath = new Path("/");
		boolean isRoot = rootPath.isRoot();
		logger.info(String.format("Path: '%s' root: '%b'", rootPath.toString(), isRoot));
		
		RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(rootPath, true);
		while (files.hasNext()) {
			LocatedFileStatus file = files.next();
			logger.info(String.format("Path: '%s'", file.getPath()));
		}
	}
}
