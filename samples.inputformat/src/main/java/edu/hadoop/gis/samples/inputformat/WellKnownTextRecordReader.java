package edu.hadoop.gis.samples.inputformat;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.Geometry.Type;
import com.esri.core.geometry.GeometryEngine;

/**
 * Read well known text line by line and emit those records as shape binary.
 * 
 * @author Jan Tschada
 *
 */
public class WellKnownTextRecordReader extends RecordReader<LongWritable, BytesWritable> {

	private LineRecordReader lineRecordReader;
	private BytesWritable bytes;
	
	private final Log logger;
	
	public WellKnownTextRecordReader() {
		logger = LogFactory.getLog(getClass());
	}

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
		lineRecordReader = new LineRecordReader();
		lineRecordReader.initialize(inputSplit, context);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!lineRecordReader.nextKeyValue()) {
			return false;
		}
		
		String line = lineRecordReader.getCurrentValue().toString();
		if (line.startsWith("POINT")) {
			try {
				Geometry geometry = GeometryEngine.geometryFromWkt(line, 0, Type.Point);
				bytes = new BytesWritable(GeometryEngine.geometryToEsriShape(geometry));
			} catch (Exception ex) {
				logger.error("Converting the well known text failed!", ex);
			}
		}
		return true;
	}
	
	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return lineRecordReader.getCurrentKey();
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		return bytes;
	}
	
	@Override
	public float getProgress() throws IOException, InterruptedException {
		return lineRecordReader.getProgress();
	}

	@Override
	public void close() throws IOException {
		lineRecordReader.close();		
	}
}
