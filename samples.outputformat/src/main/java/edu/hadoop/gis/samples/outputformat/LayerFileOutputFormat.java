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
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.hadoop.gis.samples.inputformat.ShapeWritable;

/**
 * Defines an output format for separating shapes by geometry type.
 * 
 * @author Jan Tschada
 *
 */
public class LayerFileOutputFormat extends FileOutputFormat<IntWritable, List<ShapeWritable>> {

	private final Log logger;

	public LayerFileOutputFormat() {
		logger = LogFactory.getLog(getClass());
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
			return new ShapeRecordWriter(boundingBoxOutputStream, context);
		} catch (Exception ex) {
			logger.error(String.format("Creating the bounding box file failed!", ex));
			return null;
		}
	}
}
