package edu.hadoop.gis.samples.inputformat;

import java.io.IOException;

import junit.framework.TestCase;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.Geometry.Type;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.WktImportFlags;

/**
 * Testing the well known parser.
 * 
 * @author Jan Tschada
 *
 */
public class AppTest extends TestCase {
	
	/**
	 * Tests the conversion of an OGC LINESTRING.
	 */
	public void testLinestringConversion() {
		String wkt = "LINESTRING (7.3291536 49.3562601,7.3304763 49.3569323)";
		Polyline polyline = (Polyline) GeometryEngine.geometryFromWkt(wkt, WktImportFlags.wktImportDefaults, Type.Polyline);
		assertNotNull("Polyline must not be null!", polyline);
	}
	
	/**
	 * Tests the parsing of an OGC LINESTRING.
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	public void testParseLinestring() throws IOException, InterruptedException {
		@SuppressWarnings("resource")
		WellKnownTextRecordReader reader = new WellKnownTextRecordReader();
		String wkt = "\"LINESTRING (7.3291536 49.3562601,7.3304763 49.3569323)\"";
		reader.parseRecord(wkt);
		ShapeWritable shape = reader.getCurrentValue();
		assertNotNull("The shape must not be null!", shape);
		
		Polyline polyline = (Polyline) shape.getGeometry();
		assertNotNull("Polyline must not be null!", polyline);
	}
	
	/**
	 * Tests the conversion of an OGC POLYGON.
	 */
	public void testPolygonConversion() {
		String wkt = "POLYGON ((7.2307754 49.3181337,7.2308381 49.3182195,7.2310471 49.3181547,7.2309844 49.3180689,7.2307754 49.3181337))";
		Polygon polygon = (Polygon) GeometryEngine.geometryFromWkt(wkt, WktImportFlags.wktImportDefaults, Type.Polygon);
		assertNotNull("Polygon is not initialized!", polygon);
	}
	
	/**
	 * Tests the parsing of an OGC POLYGON.
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	public void testParsePolygon() throws IOException, InterruptedException {
		@SuppressWarnings("resource")
		WellKnownTextRecordReader reader = new WellKnownTextRecordReader();
		String wkt = "\"POLYGON ((7.2307754 49.3181337,7.2308381 49.3182195,7.2310471 49.3181547,7.2309844 49.3180689,7.2307754 49.3181337))\"";
		reader.parseRecord(wkt);
		ShapeWritable shape = reader.getCurrentValue();
		assertNotNull("The shape must not be null!", shape);
		
		Polygon polygon = (Polygon) shape.getGeometry();
		assertNotNull("Polygon must not be null!", polygon);
	}
}
