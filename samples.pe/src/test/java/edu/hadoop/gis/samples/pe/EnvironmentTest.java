package edu.hadoop.gis.samples.pe;

import java.util.List;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class EnvironmentTest extends TestCase {
	/**
	 * Create the test case
	 *
	 * @param testName
	 *            name of the test case
	 */
	public EnvironmentTest(String testName) {
		super(testName);
	}

	/**
	 * @return the suite of tests being tested
	 */
	public static Test suite() {
		return new TestSuite(EnvironmentTest.class);
	}

	/**
	 * Tests the initialization and construction.
	 */
	public void testGeodesicLine() {
		ProjectionEngine engine = ProjectionEngineEnvironment.initialize();
		WGS84Point fromPoint = new WGS84Point(0.0, 0.0);
		List<WGS84Point> points = engine.constructGeodesicPoints(fromPoint, 90.0, 500, 100);
		assertNotNull("The points must not be null!", points);
		assertEquals("The number of points is invalid!", 6, points.size()); 
	}
}
