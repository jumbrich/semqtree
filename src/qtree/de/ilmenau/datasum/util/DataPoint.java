package de.ilmenau.datasum.util;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Vector;

import org.jdom.Element;

/** This class contains the coordinates of an Element. */
@SuppressWarnings("serial")
public final class DataPoint implements Serializable {

	/** Vector with names of all attributes */
	private final Vector<String> attributeNames;
	/** the according element */
	private final Element element;
	/** coordinates of the data point */
	private transient int[] coordinates = null;


	/**
	 * method that converts a Vector of Elements to a Vector of DataPoints.
	 * 
	 * @param elements Vector of Element
	 * @param attributeNames Vector with names of all attributes
	 * @return Vector of DataPoint
	 */
	public static Vector<DataPoint> convertElementsToDataPoints(Vector<Element> elements,
			Vector<String> attributeNames) {
		Vector<DataPoint> result = new Vector<DataPoint>();

		for (Element element : elements) {
			try {
				result.add(new DataPoint(element, attributeNames));
			} catch (Exception e) {
				// ignore element
			}
		}
		return result;
	}

	/**
	 * constructor.
	 * 
	 * @param element the XML element to represent
	 * @param attributeNames Vector with names of all attributes
	 */
	public DataPoint(Element element, Vector<String> attributeNames) {
		this.element = element;
		this.attributeNames = attributeNames;
	}

	/**
	 * constructor.
	 * 
	 * @param coordinates the coordinates of the point 
	 * @param parentNodeName the name of the node that elements ({@link #attributeNames}) are indexed
	 * @param attributeNames Vector with names of all attributes
	 */
	public DataPoint(int[] coordinates, String parentNodeName, Vector<String> attributeNames) {
		this.attributeNames = attributeNames;
		this.coordinates = coordinates;
		this.element = new Element(parentNodeName);
		// create sub nodes
		for (int i = 0; i < coordinates.length; i++) {
			this.element.addContent(new Element(
				attributeNames.get(i)).setText(Integer.toString(coordinates[i])
			));
		}
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object) {
		if (this == object)
			return true;
		if (object == null)
			return false;
		if (!(object instanceof DataPoint))
			return false;

		return Arrays.equals(this.coordinates, ((DataPoint) object).coordinates);
	}

	/**
	 * Returns the Vector with names of all attributes.
	 * 
	 * @return the attribute names as Vector
	 */
	public Vector<String> getAttributeNames() {
		return this.attributeNames;
	}


	/**
	 * Returns the coordinates of the data point.
	 * 
	 * @return the coordinates
	 */
	public int[] getCoordinates() {
		// create coordinates on the first use
		if (this.coordinates == null) {
			int cntDims = this.attributeNames.size();
			this.coordinates = new int[cntDims];

			for (int i = 0; i < cntDims; i++) {
				int value;
				try {
					value = Integer.parseInt(this.element.getChildTextTrim(
						this.attributeNames.get(i))
					);
				} catch (Exception e) {
					value = 0;
				}
				this.coordinates[i] = value;
			}
		}
		return this.coordinates;
	}

	/**
	 * Returns the according element.
	 * 
	 * @return the Element
	 */
	public Element getElement() {
		return this.element;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return Arrays.hashCode(this.coordinates);
	}
}