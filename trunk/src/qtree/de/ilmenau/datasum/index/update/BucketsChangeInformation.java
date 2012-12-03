/*
 *
 */
package de.ilmenau.datasum.index.update;

import java.io.Serializable;

/**
 * This class represents information about changes in buckets.
 * The information is used to calculate the index change informtion!
 * (see {@link IndexChangeInformation})
 * 
 * @author Christian Lemke
 * @version $Id$
 */
@SuppressWarnings("serial")
public final class BucketsChangeInformation implements Serializable {

	// information _together_ for the old and new index
	/** the number of all buckets (added, removed, changed, unchanged) */
	private int nmbOfBuckets;
	/** the number of changed buckets (added, removed, changed) */
	private int nmbOfChangedBuckets;
	/** measure for changing bucket size based on the average bucket length (summarized over all buckets) */
	private double totalChangedAverageBucketLengthRatio;
	/** measure for changing bucket size based on the maximum bucket length (summarized over all buckets) */
	private double totalChangedMaxBucketLengthRatio;
	/** measure for changed items in a bucket (summarized over all buckets) */
	private double totalChangedBucketCountRatio;

	// information _separately_ for the old and new index
	/** the number of buckets in the old index */
	private int nmbOfBucketsInOldIndex;
	/** the number of buckets in the new index */
	private int nmbOfBucketsInNewIndex;
	/** the average bucket length (summarized over all buckets in the old index) */
	private double totalAverageBucketLengthInOldIndex;
	/** the average bucket length (summarized over all buckets in the new index) */
	private double totalAverageBucketLengthInNewIndex;
	/** the maximum bucket length (summarized over all buckets in the old index) */
	private double totalMaxBucketLengthInOldIndex;
	/** the maximum bucket length (summarized over all buckets in the new index) */
	private double totalMaxBucketLengthInNewIndex;


	/**
	 * standard constructor.
	 */
	public BucketsChangeInformation() {
		// information _together_ for the old and new index
		this.nmbOfBuckets = 0;
		this.nmbOfChangedBuckets = 0;
		this.totalChangedAverageBucketLengthRatio = 0.0;
		this.totalChangedMaxBucketLengthRatio = 0.0;
		this.totalChangedBucketCountRatio = 0.0;
		// information _separately_ for the old and new index
		this.nmbOfBucketsInOldIndex = 0;
		this.nmbOfBucketsInNewIndex = 0;
		this.totalAverageBucketLengthInOldIndex = 0.0;
		this.totalAverageBucketLengthInNewIndex = 0.0;
		this.totalMaxBucketLengthInOldIndex = 0.0;
		this.totalMaxBucketLengthInNewIndex = 0.0;
	}

	/**
	 * constructor.
	 * 
	 * @param nmbOfBuckets the number of all buckets
	 * @param nmbOfChangedBuckets the number of changed buckets
	 * @param totalChangedAverageBucketLengthRatio measure for changing bucket size based on the average bucket length (summarized over all buckets)
	 * @param totalChangedMaxBucketLengthRatio measure for changing bucket size based on the maximum bucket length (summarized over all buckets)
	 * @param totalChangedBucketCountRatio measure for changed items in a bucket (summarized over all buckets)
	 * @param nmbOfBucketsInOldIndex the number of buckets in the old index
	 * @param nmbOfBucketsInNewIndex the number of buckets in the new index
	 * @param totalAverageBucketLengthInOldIndex the average bucket length (summarized over all buckets in the old index)
	 * @param totalAverageBucketLengthInNewIndex the average bucket length (summarized over all buckets in the new index)
	 * @param totalMaxBucketLengthInOldIndex the maximum bucket length (summarized over all buckets in the old index)
	 * @param totalMaxBucketLengthInNewIndex the maximum bucket length (summarized over all buckets in the new index)
	 */
	public BucketsChangeInformation(int nmbOfBuckets, int nmbOfChangedBuckets,
			double totalChangedBucketCountRatio, double totalChangedAverageBucketLengthRatio,
			double totalChangedMaxBucketLengthRatio, int nmbOfBucketsInOldIndex,
			int nmbOfBucketsInNewIndex, double totalAverageBucketLengthInOldIndex,
			double totalAverageBucketLengthInNewIndex, double totalMaxBucketLengthInOldIndex,
			double totalMaxBucketLengthInNewIndex) {
		// information _together_ for the old and new index
		this.nmbOfBuckets = nmbOfBuckets;
		this.nmbOfChangedBuckets = nmbOfChangedBuckets;
		this.totalChangedAverageBucketLengthRatio = totalChangedAverageBucketLengthRatio;
		this.totalChangedMaxBucketLengthRatio = totalChangedMaxBucketLengthRatio;
		this.totalChangedBucketCountRatio = totalChangedBucketCountRatio;
		// information _separately_ for the old and new index
		this.nmbOfBucketsInOldIndex = nmbOfBucketsInOldIndex;
		this.nmbOfBucketsInNewIndex = nmbOfBucketsInNewIndex;
		this.totalAverageBucketLengthInOldIndex = totalAverageBucketLengthInOldIndex;
		this.totalAverageBucketLengthInNewIndex = totalAverageBucketLengthInNewIndex;
		this.totalMaxBucketLengthInOldIndex = totalMaxBucketLengthInOldIndex;
		this.totalMaxBucketLengthInNewIndex = totalMaxBucketLengthInNewIndex;
	}

	/**
	 * method that adds change information to the current index change information.
	 * 
	 * @param changeInformation the change information to add
	 */
	public void add(BucketsChangeInformation changeInformation) {
		// information _together_ for the old and new index
		this.nmbOfBuckets += changeInformation.nmbOfBuckets;
		this.nmbOfChangedBuckets += changeInformation.nmbOfChangedBuckets;
		this.totalChangedAverageBucketLengthRatio += changeInformation.totalChangedAverageBucketLengthRatio;
		this.totalChangedMaxBucketLengthRatio += changeInformation.totalChangedMaxBucketLengthRatio;
		this.totalChangedBucketCountRatio += changeInformation.totalChangedBucketCountRatio;
		// information _separately_ for the old and new index
		this.nmbOfBucketsInOldIndex += changeInformation.nmbOfBucketsInOldIndex;
		this.nmbOfBucketsInNewIndex += changeInformation.nmbOfBucketsInNewIndex;
		this.totalAverageBucketLengthInOldIndex += changeInformation.totalAverageBucketLengthInOldIndex;
		this.totalAverageBucketLengthInNewIndex += changeInformation.totalAverageBucketLengthInNewIndex;
		this.totalMaxBucketLengthInOldIndex += changeInformation.totalMaxBucketLengthInOldIndex;
		this.totalMaxBucketLengthInNewIndex += changeInformation.totalMaxBucketLengthInNewIndex;
	}

	/**
	 * @return returns nmbOfBuckets
	 */
	public int getNmbOfBuckets() {
		return this.nmbOfBuckets;
	}

	/**
	 * @return returns nmbOfBucketsInNewIndex
	 */
	public int getNmbOfBucketsInNewIndex() {
		return this.nmbOfBucketsInNewIndex;
	}

	/**
	 * @return returns nmbOfBucketsInOldIndex
	 */
	public int getNmbOfBucketsInOldIndex() {
		return this.nmbOfBucketsInOldIndex;
	}

	/**
	 * @return returns nmbOfChangedBuckets
	 */
	public int getNmbOfChangedBuckets() {
		return this.nmbOfChangedBuckets;
	}

	/**
	 * @return returns totalAverageBucketLengthInNewIndex
	 */
	public double getTotalAverageBucketLengthInNewIndex() {
		return this.totalAverageBucketLengthInNewIndex;
	}

	/**
	 * @return returns totalAverageBucketLengthInOldIndex
	 */
	public double getTotalAverageBucketLengthInOldIndex() {
		return this.totalAverageBucketLengthInOldIndex;
	}

	/**
	 * @return returns totalChangedAverageBucketLengthRatio
	 */
	public double getTotalChangedAverageBucketLengthRatio() {
		return this.totalChangedAverageBucketLengthRatio;
	}

	/**
	 * @return returns totalChangedBucketCountRatio
	 */
	public double getTotalChangedBucketCountRatio() {
		return this.totalChangedBucketCountRatio;
	}

	/**
	 * @return returns totalChangedMaxBucketLengthRatio
	 */
	public double getTotalChangedMaxBucketLengthRatio() {
		return this.totalChangedMaxBucketLengthRatio;
	}

	/**
	 * @return returns totalMaxBucketLengthInNewIndex
	 */
	public double getTotalMaxBucketLengthInNewIndex() {
		return this.totalMaxBucketLengthInNewIndex;
	}

	/**
	 * @return returns totalMaxBucketLengthInOldIndex
	 */
	public double getTotalMaxBucketLengthInOldIndex() {
		return this.totalMaxBucketLengthInOldIndex;
	}
}
