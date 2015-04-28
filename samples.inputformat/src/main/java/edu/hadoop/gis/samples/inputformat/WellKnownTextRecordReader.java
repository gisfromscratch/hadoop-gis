package edu.hadoop.gis.samples.inputformat;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
 * The well known text can be produced using GDAL:
 * ogr2ogr -f CSV points.csv points.shp -lco GEOMETRY=AS_WKT
 * 
 * @author Jan Tschada
 *
 */
public class WellKnownTextRecordReader extends RecordReader<LongWritable, ShapeWritable> {

	private LineRecordReader lineRecordReader;
	private ShapeWritable shape;
	private static final int DefaultWkt = 0;	
	
	private static final ShapeWritable nullShape = new ShapeWritable();
	
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
		parseRecord(line);
		return true;
	}

	void parseRecord(String line) {
		Type geometryType = Type.Unknown;
		if (line.startsWith("\"POINT")) {
			geometryType = Type.Point;
		} else if (line.startsWith("\"LINESTRING") || line.startsWith("\"MULTILINESTRING")) {
			geometryType = Type.Polyline;
		} else if (line.startsWith("\"POLYGON") || line.startsWith("\"MULTILINESTRING")) {
			geometryType = Type.Polygon;
		}
		
		if (Type.Unknown != geometryType) {
			try {
				int wktStartIndex = line.indexOf("\"");
				int wktEndIndex = line.indexOf("\"", wktStartIndex + 1);
				if (-1 != wktStartIndex && -1 != wktEndIndex) {
					String wkt = line.substring(wktStartIndex + 1, wktEndIndex);
					try {
						Geometry geometry = GeometryEngine.geometryFromWkt(wkt, DefaultWkt, geometryType);
						shape = new ShapeWritable(geometry);
					} catch (Exception ex) {
						logger.error(String.format("Converting the well known text '%s' failed!", wkt), ex);
					}
				} else {
					logger.warn(String.format("'%s' cannot not be converted to a known geometry!", line));
				}
			} catch (Exception ex) {
				logger.error(String.format("Converting the well known text of line '%s' failed!", line), ex);
			}
		}
	}
	
	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return lineRecordReader.getCurrentKey();
	}

	@Override
	public ShapeWritable getCurrentValue() throws IOException, InterruptedException {
		// Never return null in here!
		return (null != shape) ? shape : nullShape;
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
