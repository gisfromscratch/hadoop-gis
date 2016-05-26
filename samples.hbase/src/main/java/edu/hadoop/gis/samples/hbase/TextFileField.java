package edu.hadoop.gis.samples.hbase;

/**
 * Represents a simple field of a text file.
 * @author Jan Tschada
 *
 */
public class TextFileField {

	private int index;
	private String name;
	
	/**
	 * Creates a new field.
	 * @param index the position of this field starting at <code>0</code>. 
	 * @param name the name of the field.
	 */
	public TextFileField(int index, String name) {
		this.index = index;
		this.name = name;
	}
	
	public int getIndex() {
		return index;
	}
	
	public void setIndex(int index) {
		this.index = index;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
}
