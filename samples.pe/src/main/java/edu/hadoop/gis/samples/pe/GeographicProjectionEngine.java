package edu.hadoop.gis.samples.pe;

import java.util.ArrayList;
import java.util.List;

import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.GeodesicData;
import net.sf.geographiclib.GeodesicLine;
import net.sf.geographiclib.GeodesicMask;

/**
 * Represents the geographic projection engine.
 * @author Jan Tschada
 *
 */
class GeographicProjectionEngine implements ProjectionEngine {

	private final double Epsilon = 1E-5;
	
	public List<WGS84Point> constructGeodesicPoints(WGS84Point fromPoint, double azimuthInDegree, double distanceInMeters,
			double segmentLength) {
		if (segmentLength < Epsilon) {
			throw new IllegalArgumentException("The segment length must be greater than 1E-5!");
		}
		if (distanceInMeters < segmentLength) {
			throw new IllegalArgumentException("The overall distance must not be shorter than the segment length!");
		}
		
		double azimuthInRadians = azimuthInDegree / 180.0 * Math.PI;
		
		List<WGS84Point> points = new ArrayList<WGS84Point>();
		points.add(fromPoint);
		GeodesicLine directLine = Geodesic.WGS84.DirectLine(fromPoint.getLatitude(), fromPoint.getLongitude(), azimuthInRadians, distanceInMeters);
		for (double distanceAlong = segmentLength; distanceAlong + Epsilon < distanceInMeters; distanceAlong+=segmentLength) {
			GeodesicData position = directLine.Position(distanceAlong, GeodesicMask.LATITUDE | GeodesicMask.LONGITUDE);
			points.add(new WGS84Point(position.lat2, position.lon2));
		}
		GeodesicData toPosition = directLine.Position(distanceInMeters, GeodesicMask.LATITUDE | GeodesicMask.LONGITUDE);
		points.add(new WGS84Point(toPosition.lat2, toPosition.lon2));
		return points;
	}

}
