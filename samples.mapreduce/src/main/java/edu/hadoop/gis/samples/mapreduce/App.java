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
import org.gdal.gdal.gdal;
import org.gdal.ogr.DataSource;
import org.gdal.ogr.Layer;
import org.gdal.ogr.ogr;

/**
 * Sample application using the File System API
 * 
 * @author Jan Tschada
 *
 */
public class App {
	
	private static final Log logger = LogFactory.getLog(App.class);
	private static final String ShapeFileExtenstion = ".shp";
	
	private static void readShapefile() {
		logger.info("Registering gdal drivers...");
		gdal.AllRegister();
		logger.info("Registering ogr drivers...");
		ogr.RegisterAll();
		
		DataSource datasource = ogr.Open("~/Downloads/Geodata/buildings.shp", false);
		int layerCount = datasource.GetLayerCount();
		logger.info(String.format("Datasource '%s' containing '%d' layers.", datasource.GetName(), layerCount));
		for (int layerIndex = 0; layerIndex < layerCount; layerIndex++) {
			Layer layer = datasource.GetLayer(layerIndex);
			logger.info(String.format("Reading layer '%s'.", layer.GetName()));
		}
	}
	
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
			Path filePath = file.getPath();
			logger.info(String.format("Path: '%s'", filePath));
		}
		
		readShapefile();
		logger.info("Done...");
	}
}
