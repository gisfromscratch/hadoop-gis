package edu.hadoop.gis.samples.inputformat;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Read well known text from an input file line by line.
 * 
 * @author Jan Tschada
 *
 */
public class WellKnownTextFileInputFormat extends FileInputFormat<LongWritable, BytesWritable> {

	private final Log logger;
	
	public WellKnownTextFileInputFormat() {
		logger = LogFactory.getLog(getClass());
	}
	
	@Override
	public RecordReader<LongWritable, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
		return new WellKnownTextRecordReader();
	}

}
