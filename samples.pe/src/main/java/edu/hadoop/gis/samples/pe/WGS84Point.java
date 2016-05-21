package edu.hadoop.gis.samples.pe;

/**
 * Represents a point having WGS84 spatial reference.
 * @author Jan Tschada
 *
 */
public class WGS84Point {

	private double latitude;
	private double longitude;

	/**
	 * Creates a new point.
	 * @param latitude the latitude
	 * @param longitude the longitude
	 */
	public WGS84Point(double latitude, double longitude) {
		this.latitude = latitude;
		this.longitude = longitude;
	}
	
	public WGS84Point() {
	}
	
	public double getLatitude() {
		return latitude;
	}
	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}
	public double getLongitude() {
		return longitude;
	}
	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}
}
