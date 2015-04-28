package edu.hadoop.gis.samples.inputformat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.Geometry.GeometryType;
import com.esri.core.geometry.Geometry.Type;
import com.esri.core.geometry.GeometryEngine;

/**
 * Shape representation of geometries.
 * 
 * @author Jan Tschada
 *
 */
public class ShapeWritable implements Writable {

	private IntWritable geometryType;
	private IntWritable bytesLength;
	private BytesWritable shape;
	
	private static final int DefaultWkt = 0;

	private final Log logger;

	public ShapeWritable() {
		logger = LogFactory.getLog(getClass());

		geometryType = new IntWritable(0);
		bytesLength = new IntWritable(0);
		shape = new BytesWritable();
	}

	public ShapeWritable(Geometry geometry) {
		this();
		setGeometry(geometry);
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(geometryType.get());
		out.writeInt(bytesLength.get());
		out.write(shape.getBytes());
	}

	public void readFields(DataInput in) throws IOException {
		geometryType.set(in.readInt());
		bytesLength.set(in.readInt());
		byte bytes[] = new byte[bytesLength.get()];
		in.readFully(bytes);
		shape.set(new BytesWritable(bytes));
	}

	public void setGeometry(Geometry geometry) {
		if (null == geometry) {
			geometryType.set(0);
			bytesLength.set(0);
			shape.set(new BytesWritable());
		} else {
			try {
				Type type = geometry.getType();
				switch (type) {
				case Point:
				case Polyline:
				case Polygon:
					geometryType.set(type.value());
					byte[] bytes = GeometryEngine.geometryToEsriShape(geometry);
					bytesLength.set(bytes.length);
					shape.set(new BytesWritable(bytes));
					break;

				default:
					logger.warn("Unsupported geometry type!");
					break;
				}
			} catch (Exception ex) {
				logger.error("Updating the geometry failed!", ex);
			}
		}
	}

	public Geometry getGeometry() {
		if (0 < bytesLength.get()) {
			try {
				switch (geometryType.get()) {
				case GeometryType.Point:
					return GeometryEngine.geometryFromEsriShape(shape.getBytes(), Type.Point);
				case GeometryType.Polyline:
					return GeometryEngine.geometryFromEsriShape(shape.getBytes(), Type.Polyline);
				case GeometryType.Polygon:
					return GeometryEngine.geometryFromEsriShape(shape.getBytes(), Type.Polygon);
				}
			} catch (Exception ex) {
				logger.error("Converting the geometry failed!");
			}
		}
		return null;
	}
	
	@Override
	public String toString() {
		Geometry geometry = getGeometry();
		if (null != geometry) {
			try {
				return GeometryEngine.geometryToWkt(geometry, DefaultWkt);
			} catch (Exception ex) {
				logger.error("Converting the geometry failed!");
			}
		}
		return "NULL";
	}
}
