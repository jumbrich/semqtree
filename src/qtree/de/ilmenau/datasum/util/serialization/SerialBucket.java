/*
 * Created: 12.04.2007
 * Changed: $Date$
 * 
 * $Log$
 * Revision 1.1  2007-04-12 18:12:04  lemke
 * + initial import
 *
 */
package de.ilmenau.datasum.util.serialization;

import java.io.Serializable;
import java.util.Vector;

/**
 * This class represents a serializable Bucket.
 * 
 * @author Christian Lemke
 * @version $Id$
 */
@SuppressWarnings("serial")
public final class SerialBucket implements Serializable {
	private static final long serialVersionUID = 1L;
	/** the coordinates of the lower boundaries */
	private int[] lowerBoundaries;
	/** the coordinates of the upper boundaries */
	private int[] upperBoundaries;
	/** the number of elements in the bucket */
	private double count;
	/** the coordinates of the contained data points */
	private Vector<int[]> containedPoints;


	/**
	 * constructor.
	 * 
	 */
	public SerialBucket() {
		this.lowerBoundaries = null;
		this.upperBoundaries = null;
		this.count = 0.0;
		this.containedPoints = null;
	}

	/**
	 * constructor.
	 * 
	 * @param lowerBoundaries the coordinates of the lower boundaries
	 * @param upperBoundaries the coordinates of the upper boundaries
	 * @param count the number of elements in the bucket
	 * @param containedPoints the coordinates of the contained data points
	 */
	public SerialBucket(int[] lowerBoundaries, int[] upperBoundaries, double count,
			Vector<int[]> containedPoints) {
		this.lowerBoundaries = lowerBoundaries;
		this.upperBoundaries = upperBoundaries;
		this.count = count;
		this.containedPoints = containedPoints;
	}

	/**
	 * @return returns containedPoints
	 */
	public Vector<int[]> getContainedPoints() {
		return this.containedPoints;
	}

	/**
	 * @return returns count
	 */
	public double getCount() {
		return this.count;
	}

	/**
	 * @return returns lowerBoundaries
	 */
	public int[] getLowerBoundaries() {
		return this.lowerBoundaries;
	}

	/**
	 * @return returns upperBoundaries
	 */
	public int[] getUpperBoundaries() {
		return this.upperBoundaries;
	}

	/**
	 * @param containedPoints sets containedPoints
	 */
	public void setContainedPoints(Vector<int[]> containedPoints) {
		this.containedPoints = containedPoints;
	}

	/**
	 * @param count sets count
	 */
	public void setCount(double count) {
		this.count = count;
	}

	/**
	 * @param lowerBoundaries sets lowerBoundaries
	 */
	public void setLowerBoundaries(int[] lowerBoundaries) {
		this.lowerBoundaries = lowerBoundaries;
	}

	/**
	 * @param upperBoundaries sets upperBoundaries
	 */
	public void setUpperBoundaries(int[] upperBoundaries) {
		this.upperBoundaries = upperBoundaries;
	}
}
