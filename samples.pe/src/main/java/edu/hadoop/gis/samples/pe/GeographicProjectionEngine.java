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

	public List<WGS84Point> constructGeodesicPoints(WGS84Point fromPoint, WGS84Point toPoint, double segmentLength) {
		if (segmentLength < Epsilon) {
			throw new IllegalArgumentException("The segment length must be greater than 1E-5!");
		}
		
		GeodesicLine inverseLine = Geodesic.WGS84.InverseLine(fromPoint.getLatitude(), fromPoint.getLongitude(), toPoint.getLatitude(), toPoint.getLongitude(), GeodesicMask.DISTANCE_IN);
		double distanceInMeters = inverseLine.Distance();
		List<WGS84Point> points = new ArrayList<WGS84Point>();
		points.add(fromPoint);
		if (distanceInMeters < segmentLength) {
			// Segment cannot be greater than the overall length
			points.add(toPoint);
			return points;
		}
		
		for (double distanceAlong = segmentLength; distanceAlong + Epsilon < distanceInMeters; distanceAlong+=segmentLength) {
			GeodesicData position = inverseLine.Position(distanceAlong, GeodesicMask.LATITUDE | GeodesicMask.LONGITUDE);
			points.add(new WGS84Point(position.lat2, position.lon2));
		}
		points.add(toPoint);
		return points;
	}

}
