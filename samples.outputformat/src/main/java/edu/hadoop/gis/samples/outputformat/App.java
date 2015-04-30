package edu.hadoop.gis.samples.outputformat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;

import com.sun.activation.viewers.TextViewer;

import edu.hadoop.gis.samples.inputformat.GeometryTypeMapper;
import edu.hadoop.gis.samples.inputformat.ShapeWritable;
import edu.hadoop.gis.samples.inputformat.WellKnownTextFileInputFormat;

/**
 * Sample application using a custom output format
 * 
 * @author Jan Tschada
 *
 */
public class App {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		Job outputJob = Job.getInstance(configuration, "custom-output");

		outputJob.setJarByClass(App.class);
		outputJob.setInputFormatClass(WellKnownTextFileInputFormat.class);
		outputJob.setMapperClass(GeometryTypeMapper.class);
		outputJob.setMapOutputKeyClass(IntWritable.class);
		outputJob.setMapOutputValueClass(ShapeWritable.class);
		
		outputJob.setOutputFormatClass(LayerFileOutputFormat.class);

		FileInputFormat.setInputPaths(outputJob, "/input/custom-input");
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyy-MM-dd'T'HHmmss");
		UUID uniqueId = UUID.randomUUID();
		String folderName = String.format("%s-%s", dateFormat.format(new Date()), uniqueId.toString());
		FileOutputFormat.setOutputPath(outputJob, new Path("/output", folderName));
		System.exit(outputJob.waitForCompletion(true) ? 0 : 1);
	}
}
