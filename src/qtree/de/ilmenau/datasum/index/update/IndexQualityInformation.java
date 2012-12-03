/*
 *
 */
package de.ilmenau.datasum.index.update;

import java.io.Serializable;

/**
 * This class contains measures about the index quality.
 * 
 * @author Christian Lemke
 * @version $Id$
 */
@SuppressWarnings("serial")
public final class IndexQualityInformation implements Serializable {

	/**
	 * measure:  average length of a bucket
	 * <ul>
	 * 	<li>averaged over all buckets in the index</li>
	 * 	<li>the less the value the better the approximation quality</li>
	 * </ul>
	 */
	private double averageBucketlength;
	/**
	 * measure:  ratio of the 'number of unique elements in a bucket' to the 'capacity of the bucket'
	 * <ul>
	 * 	<li>averaged over all buckets in the index</li>
	 * 	<li>the higher the value the better the space in the bucket is utilized</li>
	 * </ul>
	 */
	private double bucketDensity;
	/**
	 * measure:  maximum distance (approximation error) from a point to a corner of the bucket
	 * <ul>
	 * 	<li>averaged over all points (elements) in a bucket</li>
	 * 	<li>averaged over all buckets in the index</li>
	 * 	<li>the less the value the better the approximation quality</li>
	 * </ul>
	 */
	private double maximumDistance;


	/**
	 * standard constructor.
	 */
	public IndexQualityInformation() {
		this.averageBucketlength = 0.0;
		this.bucketDensity = 0.0;
		this.maximumDistance = 0.0;
	}

	/**
	 * constructor.
	 * 
	 * @param averageBucketlength how big an average bucket is
	 * @param bucketDensity how good the space in a bucket is utilized
	 * @param maximumDistance how high the maximum approximation error is
	 */
	public IndexQualityInformation(double averageBucketlength, double bucketDensity,
			double maximumDistance) {
		this.averageBucketlength = averageBucketlength;
		this.bucketDensity = bucketDensity;
		this.maximumDistance = maximumDistance;
	}

	/**
	 * @return returns averageBucketlength
	 */
	public double getAverageBucketlength() {
		return this.averageBucketlength;
	}

	/**
	 * @return returns how good the space in a bucket is utilized
	 */
	public double getBucketDensity() {
		return this.bucketDensity;
	}

	/**
	 * @return returns how high the maximum approximation error is
	 */
	public double getMaximumDistance() {
		return this.maximumDistance;
	}

	/**
	 * @param averageBucketlength sets averageBucketlength
	 */
	public void setAverageBucketlength(double averageBucketlength) {
		this.averageBucketlength = averageBucketlength;
	}

	/**
	 * @param bucketDensity sets bucketDensity
	 */
	public void setBucketDensity(double bucketDensity) {
		this.bucketDensity = bucketDensity;
	}

	/**
	 * @param maximumDistance sets maximumDistance
	 */
	public void setMaximumDistance(double maximumDistance) {
		this.maximumDistance = maximumDistance;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@SuppressWarnings("nls")
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("IndexQualityInformation:\n\taverage length (averaged) = ");
		sb.append(this.averageBucketlength);
		sb.append("\n\tbucket densitiy (averaged) = ");
		sb.append(this.bucketDensity);
		sb.append("\n\tmaximum distance (averaged) = ");
		sb.append(this.maximumDistance);
		sb.append("\n");
		return sb.toString();
	}
}
