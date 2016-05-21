package edu.hadoop.gis.samples.pe;

import java.util.List;

/**
 * Provides the projection engine functions.
 * @author Jan Tschada
 *
 */
public interface ProjectionEngine {

	/**
	 * Constructs point on along the geodesic arc.
	 * @param fromPoint the start point.
	 * @param azimuthInDegree the azimuth.
	 * @param distanceInMeters the distance in meters.
	 * @param segmentLength the length of each segment.
	 * @return the points along the geodesic arc.
	 */
	List<WGS84Point> constructGeodesicPoints(WGS84Point fromPoint, double azimuthInDegree, double distanceInMeters, double segmentLength);
}
