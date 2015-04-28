package edu.hadoop.gis.samples.inputformat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Sample application using a custom file input format.
 * 
 * @author Jan Tschada
 *
 */
public class App {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		Job inputJob = Job.getInstance(configuration, "custom-input");
		
		inputJob.setJarByClass(App.class);
		inputJob.setInputFormatClass(WellKnownTextFileInputFormat.class);
		inputJob.setMapperClass(GeometryTypeMapper.class);
		inputJob.setMapOutputKeyClass(IntWritable.class);
		inputJob.setMapOutputValueClass(ShapeWritable.class);
		
		FileInputFormat.setInputPaths(inputJob, "/input/custom-input");
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyy-MM-dd'T'HHmmss");
		UUID uniqueId = UUID.randomUUID();
		String folderName = String.format("%s-%s", dateFormat.format(new Date()), uniqueId.toString());
		FileOutputFormat.setOutputPath(inputJob, new Path("/output", folderName));
		System.exit(inputJob.waitForCompletion(true) ? 0 : 1);
	}
}
