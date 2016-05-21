package edu.hadoop.gis.samples.pe;

import com.sun.jna.Native;

/**
 * Represents the projection engine environment.
 * @author Jan Tschada
 *
 */
public class ProjectionEngineEnvironment {

	private static ProjectionEngine engine;
	
	private ProjectionEngineEnvironment(){
	}
	
	/**
	 * Initializes the environment.
	 */
	public static ProjectionEngine initialize() {
		if (null == engine) {
			engine = new GeographicProjectionEngine();
		}
		return engine;
	}
}
