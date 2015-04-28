package edu.hadoop.gis.samples.inputformat;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.Geometry.Type;

/**
 * Maps shapes by their geometry types.
 * 
 * @author Jan Tschada
 *
 */
public class GeometryTypeMapper extends Mapper<LongWritable, ShapeWritable, IntWritable, ShapeWritable> {

	private IntWritable geometryType;
	
	private final Log logger;
	
	public GeometryTypeMapper() {
		logger = LogFactory.getLog(getClass());
		geometryType = new IntWritable(0);
	}
	
	@Override
	protected void map(LongWritable key, ShapeWritable value, Mapper<LongWritable, ShapeWritable, IntWritable, ShapeWritable>.Context context) throws IOException, InterruptedException {
		Geometry geometry = value.getGeometry();
		if (null == geometry) {
			logger.warn("Shape has no geometry it can not be mapped!");
			return;
		}
		
		Type type = geometry.getType();
		switch (type) {
		case Point:
		case Polyline:
		case Polygon:
			geometryType.set(type.value());
			context.write(geometryType, value);
			break;
		default:
			logger.warn("Unsupported geometry type can not be mapped!");
			break;
		}
	}
}
