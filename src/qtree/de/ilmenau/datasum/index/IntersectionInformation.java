/*
 *
 */
package de.ilmenau.datasum.index;

import java.io.Serializable;

import de.ilmenau.datasum.util.bigmath.Space;

/**
 * This class contains information about the intersection of a query space and a bucket.
 * 
 * @author Christian Lemke
 * @version $Id$
 */
public final class IntersectionInformation implements Serializable{
	private static final long serialVersionUID = 1L;
	/** the corresponding bucket */
	private Bucket correspondingBucket;
	/** the intersected space */
	private Space intersection;
	/** the ratio between the intersection and the total size of the bucket */
	private double intersectionRatio;


	/**
	 * constructor.
	 * 
	 * @param correspondingBucket the corresponding bucket
	 * @param intersection the intersected space
	 * @param intersectionRatio the ratio between the intersection and the total size of the bucket
	 */
	public IntersectionInformation(Bucket correspondingBucket, Space intersection,
			double intersectionRatio) {
		this.correspondingBucket = correspondingBucket;
		this.intersection = intersection;
		this.intersectionRatio = intersectionRatio;
	}

	/**
	 * @return returns the corresponding bucket
	 */
	public Bucket getCorrespondingBucket() {
		return this.correspondingBucket;
	}

	/**
	 * method that returns the estimated count of elements in the bucket for the given query space.
	 * (intersection ratio * bucket.count)
	 * 
	 * @return the estimated count of elements in the bucket
	 */
	public double getEstimatedBucketCount() {
		return this.intersectionRatio * this.correspondingBucket.getCount();
	}

	/**
	 * @return returns the intersected space
	 */
	public Space getIntersection() {
		return this.intersection;
	}

	/**
	 * @return returns the ratio between the intersection and the total size of the bucket
	 */
	public double getIntersectionRatio() {
		return this.intersectionRatio;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();

		sb.append(this.correspondingBucket);
		sb.append("   intersection ratio: "); //$NON-NLS-1$
		sb.append(this.intersectionRatio);
		sb.append("   intersection: "); //$NON-NLS-1$
		sb.append(this.intersection);

		return sb.toString();
	}
}
