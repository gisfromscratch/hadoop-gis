package edu.hadoop.gis.samples.inputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/**
 * Sample application using a custom file input format.
 * 
 * @author Jan Tschada
 *
 */
public class App {
	
	public static void main(String[] args) throws IOException {
		Configuration configuration = new Configuration();
		Job inputJob = Job.getInstance(configuration, "custom-input");
		
		inputJob.setJarByClass(App.class);
		inputJob.setInputFormatClass(WellKnownTextFileInputFormat.class);
		inputJob.setMapperClass(GeometryTypeMapper.class);
	}
}
