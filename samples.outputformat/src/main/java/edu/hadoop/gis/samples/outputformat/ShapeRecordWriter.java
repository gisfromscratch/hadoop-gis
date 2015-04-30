package edu.hadoop.gis.samples.outputformat;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.Geometry.GeometryType;

import edu.hadoop.gis.samples.inputformat.ShapeWritable;

/**
 * Writes shapes directly into the filesystem. Supports API level v1 and v2.
 * 
 * @author Jan Tschada
 *
 */
public class ShapeRecordWriter extends RecordWriter<IntWritable, List<ShapeWritable>> implements org.apache.hadoop.mapred.RecordWriter<IntWritable, List<ShapeWritable>> {

	private final DataOutputStream boundingBoxOutputStream;
	private final Envelope boundingBox;
	private final Envelope queryEnvelope;
	private final FileSystemOutput fileSystemOutput;
	private final Map<IntWritable, DataOutputStream> outputStreams;

	private final Log logger;

	public ShapeRecordWriter(DataOutputStream boundingBoxOutputStream, FileSystemOutput fileSystemOutput) {
		logger = LogFactory.getLog(getClass());
		this.boundingBoxOutputStream = boundingBoxOutputStream;
		boundingBox = new Envelope();
		queryEnvelope = new Envelope();
		this.fileSystemOutput = fileSystemOutput;
		outputStreams = new java.util.HashMap<IntWritable, DataOutputStream>();
	}

	public void write(IntWritable geometryType, List<ShapeWritable> shapes) throws IOException {
		DataOutputStream outputStream = null;
		if (outputStreams.containsKey(geometryType)) {
			outputStream = outputStreams.get(geometryType);
		} else {
			outputStream = createOutput(geometryType);
			if (null != outputStream) {
				outputStreams.put(geometryType, outputStream);
			}
		}

		if (null != outputStream) {
			for (ShapeWritable shape : shapes) {
				shape.write(outputStream);
				Geometry geometry = shape.getGeometry();
				if (null != geometry) {
					// Merge bounding box
					geometry.queryEnvelope(queryEnvelope);
					boundingBox.merge(queryEnvelope);
				}
			}
		}
	}

	private DataOutputStream createOutput(IntWritable geometryType) {
		String fileName = null;
		switch (geometryType.get()) {
		case GeometryType.Point:
			fileName = "points.lyr";
			break;
		case GeometryType.Polyline:
			fileName = "lines.lyr";
			break;
		case GeometryType.Polygon:
			fileName = "polygons.lyr";
			break;
		default:
			logger.warn("Geometry type is not supported!");
			logger.warn("No output file will be created!");
			return null;
		}

		Path outputPath = fileSystemOutput.getOutputPath();
		Path layerOutputPath = new Path(outputPath, fileName);
		try {
			FileSystem fileSystem = fileSystemOutput.getFileSystem();
			return fileSystem.create(layerOutputPath);
		} catch (IOException ex) {
			logger.error("Creating layer file failed!", ex);
			return null;
		}
	}

	/**
	 * Closing the underyling stream using v1 level API.
	 */
	public void close(TaskAttemptContext context) throws IOException {
		for (DataOutputStream outputStream : outputStreams.values()) {
			try {
				outputStream.close();
			} catch (IOException ex) {
				logger.error("Closing layer file failed!", ex);
			}
		}

		try {
			String boundingBoxAsJson = String.format("{ \"xmin\" : %f, \"ymin\" : %f, \"xmax\" : %f, \"ymax\" : %f }", boundingBox.getXMin(), boundingBox.getYMin(), boundingBox.getXMax(),
					boundingBox.getYMax());
			boundingBoxOutputStream.writeUTF(boundingBoxAsJson);
			boundingBoxOutputStream.close();
		} catch (IOException ex) {
			logger.error("Closing the *.bbox file failed!");
		}
	}

	/**
	 * Closing the underlying stream using v2 level API.
	 */
	public void close(Reporter reporter) throws IOException {
		for (DataOutputStream outputStream : outputStreams.values()) {
			try {
				outputStream.close();
			} catch (IOException ex) {
				logger.error("Closing layer file failed!", ex);
			}
		}

		try {
			String boundingBoxAsJson = String.format("{ \"xmin\" : %f, \"ymin\" : %f, \"xmax\" : %f, \"ymax\" : %f }", boundingBox.getXMin(), boundingBox.getYMin(), boundingBox.getXMax(),
					boundingBox.getYMax());
			boundingBoxOutputStream.writeUTF(boundingBoxAsJson);
			boundingBoxOutputStream.close();
		} catch (IOException ex) {
			logger.error("Closing the *.bbox file failed!");
		}
	}
}
