/*
 *
 */
package de.ilmenau.datasum.util.bigmath;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Vector;

import de.ilmenau.datasum.util.DataPoint;
import de.ilmenau.datasum.util.StringHelper;


/**
 * This class represents a space (multidimensional region).
 * 
 * @author Christian Lemke
 * @version $Id$
 */
@SuppressWarnings("serial")
public class Space implements Serializable {
	private static final long serialVersionUID = 1L;
	/** the lower boundaries of the space */
	protected int[] lowerBoundaries;
	/** the upper boundaries of the space */
	protected int[] upperBoundaries;
	/** the data points in this space -> used for evaluation */
	private Vector<DataPoint> containedPoints = null;


	/**
	 * method that calculates the maximum distance between a data point and the given space.
	 * For every corner of this space the distance to the data point is calculated and the
	 * maximum of all distances is the result. 
	 * 
	 * @param point the DataPoint
	 * @param lowerBoundaries the lower boundaries of the space
	 * @param upperBoundaries the upper boundaries of the space
	 * @return the maximum distance (as double)
	 */
	public static double calculateMaxDistance(DataPoint point, int[] lowerBoundaries,
			int[] upperBoundaries) {
		int[] pointCoordinates = point.getCoordinates();

		long lowerdiff;
		long upperdiff;
		long diff;
		double maximumDistance = 0;
		for (int i = 0; i < lowerBoundaries.length; i++) {
			lowerdiff = pointCoordinates[i] - lowerBoundaries[i];
			upperdiff = upperBoundaries[i] - pointCoordinates[i];
			diff = Math.max(lowerdiff, upperdiff);
			maximumDistance += diff * diff;
		}
		return Math.sqrt(maximumDistance);
	}

	/**
	 * constructor.
	 * 
	 * @param lowerBoundaries the lower boundaries of the space
	 * @param upperBoundaries the upper boundaries of the space
	 */
	public Space(int[] lowerBoundaries, int[] upperBoundaries) {
		this.lowerBoundaries = lowerBoundaries;
		this.upperBoundaries = upperBoundaries;
	}

	/**
	 * method that adds a data point to the space.
	 * (used for evaluation)
	 * 
	 * @param point the point
	 */
	public void addPoint(DataPoint point) {
		if (point == null) {
			return;
		}
		if (this.containedPoints == null) {
			this.containedPoints = new Vector<DataPoint>();
		}
		this.containedPoints.add(point);
	}

	/**
	 * method that adds data points to the space.
	 * (used for evaluation)
	 * 
	 * @param points the points
	 */
	public void addPoints(Vector<DataPoint> points) {
		if (points == null) {
			return;
		}
		if (this.containedPoints == null) {
			this.containedPoints = new Vector<DataPoint>();
		}
		this.containedPoints.addAll(points);
	}

	/**
	 * method that calculates the capacity of the space.
	 * 
	 * @param result the result holder (will contain the capacity after calculation)
	 */
	public void calculateCapacity(BigUInt result) {
		result.setOne(); // result = 1
		int nmbOfDimensions = this.lowerBoundaries.length;
		long lengthInDimension;
		for (int i = 0; i < nmbOfDimensions; i++) {
			lengthInDimension = ((long) this.upperBoundaries[i]) - ((long) this.lowerBoundaries[i])
					+ 1L;
			result.partialMul(lengthInDimension);
		}
	}

	/**
	 * method that calculates the maximum distance between a data point and this space.
	 * For every corner of this space the distance to the data point is calculated and the
	 * maximum of all distances is the result. 
	 * 
	 * @param point the DataPoint
	 * @return the maximum distance (as double)
	 */
	public double calculateMaxDistance(DataPoint point) {
		return calculateMaxDistance(point, this.lowerBoundaries, this.upperBoundaries);
	}

	/**
	 * method that calculates the maximum distance between a data point and this space.
	 * For every corner of this space the distance to the data point is calculated and the
	 * maximum of all distances is the result. 
	 * 
	 * @param point the DataPoint
	 * @param result the maximum distance (as BigUInt that will be filled)
	 */
	public void calculateMaxDistance(DataPoint point, BigUInt result) {
		int[] pointCoordinates = point.getCoordinates();
		result.setZero();

		long lowerdiff;
		long upperdiff;
		long diff;
		for (int i = 0; i < this.lowerBoundaries.length; i++) {
			lowerdiff = pointCoordinates[i] - this.lowerBoundaries[i];
			upperdiff = this.upperBoundaries[i] - pointCoordinates[i];
			diff = Math.max(lowerdiff, upperdiff);
			diff = diff * diff;
			result.add(diff);
		}
		result.shiftLeft(BigMathHelper.FIXPOINT_RANGE_EXTENSION * 8);
		try {
			result.sqrt(Long.MAX_VALUE);
		} catch (ArithmeticException ae) {
			// ignore -> sqrt(0)
		}
	}

	/**
	 * method that determines whether this space contains the given point.
	 * 
	 * @param point the point to test
	 * @return <tt>true</tt> if the point is in this space
	 */
	public boolean contains(DataPoint point) {
		int[] coordinates = point.getCoordinates();

		for (int i = 0; i < coordinates.length; i++) {
			if (coordinates[i] < this.lowerBoundaries[i]
					|| coordinates[i] > this.upperBoundaries[i]) {
				return false;
			}
		}
		return true;
	}

	/**
	 * method that returns whether this Space contains the given Space.
	 * 
	 * @param space the Space to test
	 * @return <tt>true</tt> if this Space contains the given Space
	 */
	public boolean contains(Space space) {
		int nmbOfDimensions = this.lowerBoundaries.length;
		for (int i = 0; i < nmbOfDimensions; i++) {
			if (this.lowerBoundaries[i] > space.lowerBoundaries[i]
					|| this.upperBoundaries[i] < space.upperBoundaries[i]) {
				return false;
			}
		}
		return true;
	}

	/**
	 * @return returns the points that are in the space
	 */
	public Vector<DataPoint> getContainedPoints() {
		return this.containedPoints;
	}

	/**
	 * @return returns the lower boundaries of the query space
	 */
	public int[] getLowerBoundaries() {
		return this.lowerBoundaries;
	}

	/**
	 * method that returns the number of unique points in the space.
	 * 
	 * @param result the result holder (will contain the number of unique points after calculation)
	 */
	public void getNmbOfUniquePoints(BigUInt result) {
		result.setZero();
		if (this.containedPoints.isEmpty()) {
			return;
		}
		
		Vector<DataPoint> uniquePoints = new Vector<DataPoint>();
		for (DataPoint point : this.containedPoints) {
			if (!uniquePoints.contains(point)) {
				uniquePoints.add(point);
			}
		}
		result.add(uniquePoints.size());
	}

	/**
	 * @return returns the upper boundaries of the query space
	 */
	public int[] getUpperBoundaries() {
		return this.upperBoundaries;
	}

	/**
	 * method that calculates the intersection of the current space with the given space and return
	 * the intersection as new space.
	 * 
	 * @param space the space to intersect with
	 * @return the intersection or <tt>null</tt> if that does not exists
	 */
	public Space intersect(Space space) {
		int nmbOfDimensions = this.lowerBoundaries.length;
		int[] lowerResultBoundaries = new int[nmbOfDimensions];
		int[] upperResultBoundaries = new int[nmbOfDimensions];

		for (int i = 0; i < nmbOfDimensions; i++) {
			lowerResultBoundaries[i] = Math.max(this.lowerBoundaries[i], space.lowerBoundaries[i]);
			upperResultBoundaries[i] = Math.min(this.upperBoundaries[i], space.upperBoundaries[i]);
			if (upperResultBoundaries[i] < lowerResultBoundaries[i]) {
				// this occurs when there is no intersection
				return null;
			}
		}
		return new Space(lowerResultBoundaries, upperResultBoundaries);
	}

	/**
	 * method that removes the given point from the contained points.
	 * 
	 * @param pointToRemove the point to remove from the contained points
	 */
	public void removePoint(DataPoint pointToRemove) {
		this.containedPoints.remove(pointToRemove);
	}

	/**
	 * method that removes all points from this space that are in the given space.
	 * 
	 * @param spaceToClear the space to clear (in this bucket)
	 */
	public void removePoints(Space spaceToClear) {
		Iterator<DataPoint> it = this.containedPoints.iterator();
		while (it.hasNext()) {
			if (spaceToClear.contains(it.next())) {
				it.remove();
			}
		}
	}

	/**
	 * method that removes the given points from the contained points.
	 * 
	 * @param pointsToRemove the points to remove from the contained points
	 */
	public void removePoints(Vector<DataPoint> pointsToRemove) {
		for (DataPoint point : pointsToRemove) {
			this.containedPoints.remove(point);
		}
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();

		sb.append(StringHelper.arrayToString(this.lowerBoundaries));
		sb.append("-"); //$NON-NLS-1$
		sb.append(StringHelper.arrayToString(this.upperBoundaries));

		return sb.toString();
	}
}
