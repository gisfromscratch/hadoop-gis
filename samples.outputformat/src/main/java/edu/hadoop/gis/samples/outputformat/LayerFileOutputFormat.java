package edu.hadoop.gis.samples.outputformat;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;

import edu.hadoop.gis.samples.inputformat.ShapeWritable;

/**
 * Defines an output format for separating shapes by geometry type.
 * Supports API level v1 and v2.
 * 
 * @author Jan Tschada
 *
 */
public class LayerFileOutputFormat extends FileOutputFormat<IntWritable, List<ShapeWritable>> implements OutputFormat<IntWritable, List<ShapeWritable>> {

	private final LayerFileOutputFormatVersionTwo newFormat;
	
	private final Log logger;

	public LayerFileOutputFormat() {
		logger = LogFactory.getLog(getClass());
		newFormat = new LayerFileOutputFormatVersionTwo();
	}

	@Override
	public RecordWriter<IntWritable, List<ShapeWritable>> getRecordWriter(TaskAttemptContext context) throws IOException {
		Configuration configuration = context.getConfiguration();
		FileSystem fileSystem = FileSystem.get(configuration);
		Path outputPath = FileOutputFormat.getOutputPath(context);

		// Create a file containing the bounding box of all shapes
		Path boundingBoxPath = new Path(outputPath, "layers.bbox");
		try {
			FSDataOutputStream boundingBoxOutputStream = fileSystem.create(boundingBoxPath, context);
			FileSystemOutput fileSystemOutput = new FileSystemOutput(fileSystem, outputPath);
			return new ShapeRecordWriter(boundingBoxOutputStream, fileSystemOutput);
		} catch (Exception ex) {
			logger.error(String.format("Creating the bounding box file failed!", ex));
			return null;
		}
	}

	public void checkOutputSpecs(FileSystem fileSystem, JobConf configuration) throws IOException {
		newFormat.checkOutputSpecs(fileSystem, configuration);
	}

	public org.apache.hadoop.mapred.RecordWriter<IntWritable, List<ShapeWritable>> getRecordWriter(FileSystem fileSystem, JobConf configuration, String name, Progressable progress) throws IOException {
		return newFormat.getRecordWriter(fileSystem, configuration, name, progress);
	}
	
	
	/**
	 * Version 2.x implementation of this file output format.
	 * 
	 * @author Jan Tschada
	 *
	 */
	private class LayerFileOutputFormatVersionTwo extends org.apache.hadoop.mapred.FileOutputFormat<IntWritable, List<ShapeWritable>> {

		@Override
		public org.apache.hadoop.mapred.RecordWriter<IntWritable, List<ShapeWritable>> getRecordWriter(FileSystem fileSystem, JobConf configuration, String name, Progressable progress) throws IOException {
			Path outputPath = org.apache.hadoop.mapred.FileOutputFormat.getOutputPath(configuration);
			
			// Create a file containing the bounding box of all shapes
			Path boundingBoxPath = new Path(outputPath, "layers.bbox");
			try {
				FileSystemOutput fileSystemOutput = new FileSystemOutput(fileSystem, outputPath);
				FSDataOutputStream boundingBoxOutputStream = fileSystem.create(boundingBoxPath, progress);
				return new ShapeRecordWriter(boundingBoxOutputStream, fileSystemOutput);
			} catch (Exception ex) {
				logger.error(String.format("Creating the bounding box file failed!", ex));
				return null;
			}
		}
	}
}
