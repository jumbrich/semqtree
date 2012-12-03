/*
 *
 */
package de.ilmenau.datasum.index.update;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

/**
 * This class represents information about an index change.
 * The information is used to decide when to aggregate and propagate updates!
 * 
 * @author Christian Lemke
 * @version $Id$
 */
@SuppressWarnings("serial")
public final class IndexChangeInformation implements Serializable {

	// information _together_ for the old and new index
	/** measure for the changed bucket size based on the average bucket length */
	private Vector<Double> changedAvgBucketlengthRatios;
	/** measure for the changed bucket size based on the maximum bucket length */
	private Vector<Double> changedMaxBucketlengthRatios;
	/** measure for the changed item count in a bucket */
	private Vector<Double> changedBucketcountRatios;
	/** the weighted average over the changes of all buckets (count and length) */
	private Vector<Double> weightedChangerates;
	/** the ratio of changed buckets to all buckets */
	private Vector<Double> bucketsChangedRatio;

	// information _separately_ for the old and new index
	/** change rate based on the average bucket length between the new and the old index */
	private Vector<Double> avgBucketlengthChangerates;
	/** change rate based on the maximum bucket length between the new and the old index */
	private Vector<Double> maxBucketlengthChangerates;

	// information not related to buckets
	/** the number of waiting updates */
	private int waitingUpdates;
	/** needed by update aggregation for estimation */
	private int nmbOfChangedBuckets;

	// TODO <KH>: Parameter in Konfigurationsdatei auslagern (oder entfernen)
	/** @deprecated weight for the totalChangedMaxBucketLengthRatio */
	private final transient double BUCKET_LENGTH_WEIGHT = 0.5;
	/**  @deprecated weight for the totalChangedBucketCountRatio */
	private final transient double BUCKET_COUNT_WEIGHT = 0.5;

	/** list of supported measures */
	public enum Measure {
		/** measure for the changed bucket size based on the average bucket length */
		CHANGED_AVG_BUCKETLENGTH_RATIO,
		/** measure for the changed bucket size based on the maximum bucket length */
		CHANGED_MAX_BUCKETLENGTH_RATIO,
		/** measure for the changed item count in a bucket */
		CHANGED_BUCKETCOUNT_RATIO,
		/**
		 * the weighted average over the changes of all buckets (count and length)
		 * @deprecated 
		 */
		WEIGHTED_CHANGERATE,
		/** the ratio of changed buckets to all buckets */
		BUCKETS_CHANGED_RATIO,
		/** change rate based on the average bucket length between the new and the old index */
		AVG_BUCKETLENGTH_CHANGERATE,
		/** change rate based on the maximum bucket length between the new and the old index */
		MAX_BUCKETLENGTH_CHANGERATE,
		/** the number of waiting updates */
		WAITING_UPDATES
	}


	/**
	 * standard constructor.
	 */
	public IndexChangeInformation() {
		this.changedAvgBucketlengthRatios = new Vector<Double>();
		this.changedMaxBucketlengthRatios = new Vector<Double>();
		this.changedBucketcountRatios = new Vector<Double>();
		this.weightedChangerates = new Vector<Double>();
		this.bucketsChangedRatio = new Vector<Double>();
		this.avgBucketlengthChangerates = new Vector<Double>();
		this.maxBucketlengthChangerates = new Vector<Double>();
		this.waitingUpdates = 0;
		this.nmbOfChangedBuckets = 0;
	}

	/**
	 * constructor.
	 * 
	 * @param bci the buckets change information needed for the index change information 
	 * @param waitingUpdates the number of waiting updates
	 */
	public IndexChangeInformation(BucketsChangeInformation bci, int waitingUpdates) {
		this.changedAvgBucketlengthRatios = new Vector<Double>();
		this.changedMaxBucketlengthRatios = new Vector<Double>();
		this.changedBucketcountRatios = new Vector<Double>();
		this.weightedChangerates = new Vector<Double>();
		this.bucketsChangedRatio = new Vector<Double>();
		this.avgBucketlengthChangerates = new Vector<Double>();
		this.maxBucketlengthChangerates = new Vector<Double>();

		if (bci.getNmbOfChangedBuckets() > 0) {
			this.changedAvgBucketlengthRatios.add(
				bci.getTotalChangedAverageBucketLengthRatio() / bci.getNmbOfBuckets()
			);
			this.changedMaxBucketlengthRatios.add(
				bci.getTotalChangedMaxBucketLengthRatio() / bci.getNmbOfBuckets()
			);
			this.changedBucketcountRatios.add(
				bci.getTotalChangedBucketCountRatio() / bci.getNmbOfBuckets()
			);
			this.weightedChangerates.add(
				(   (this.BUCKET_LENGTH_WEIGHT * bci.getTotalChangedMaxBucketLengthRatio())
				  + (this.BUCKET_COUNT_WEIGHT * bci.getTotalChangedBucketCountRatio())
				) / bci.getNmbOfBuckets()
			);
			this.bucketsChangedRatio.add(
				((double) bci.getNmbOfChangedBuckets()) / ((double) bci.getNmbOfBuckets())
			);
		} else {
			this.changedAvgBucketlengthRatios.add(1.0);
			this.changedMaxBucketlengthRatios.add(1.0);
			this.changedBucketcountRatios.add(1.0);
			this.weightedChangerates.add(1.0);
			this.bucketsChangedRatio.add(0.0);
		}
		this.avgBucketlengthChangerates.add(getAverageBucketLengthChangeRate(bci));
		this.maxBucketlengthChangerates.add(getMaximumBucketLengthChangeRate(bci));
		this.waitingUpdates = waitingUpdates;
		this.nmbOfChangedBuckets = bci.getNmbOfChangedBuckets();
	}

	/**
	 * method that adds an index change information to the current index change information.
	 * 
	 * @param ici the index change information to add
	 */
	public void add(IndexChangeInformation ici) {
		this.changedAvgBucketlengthRatios.addAll(ici.changedAvgBucketlengthRatios);
		this.changedMaxBucketlengthRatios.addAll(ici.changedMaxBucketlengthRatios);
		this.changedBucketcountRatios.addAll(ici.changedBucketcountRatios);
		this.weightedChangerates.addAll(ici.weightedChangerates);
		this.bucketsChangedRatio.addAll(ici.bucketsChangedRatio);
		this.avgBucketlengthChangerates.addAll(ici.avgBucketlengthChangerates);
		this.maxBucketlengthChangerates.addAll(ici.maxBucketlengthChangerates);
		this.waitingUpdates += ici.waitingUpdates;
		this.nmbOfChangedBuckets += ici.nmbOfChangedBuckets;
	}

	/**
	 * @return returns avgBucketlengthChangerates
	 */
	public Vector<Double> getAvgBucketlengthChangerates() {
		return this.avgBucketlengthChangerates;
	}

	/**
	 * method that returns the averaged value of the given measure.
	 * 
	 * @param measure the measure to retrieve
	 * @return the double value of the measure
	 */
	public double getAvgMeasure(Measure measure) {
		Vector<Double> measureValues;
		double value;
		switch (measure) {
			case WAITING_UPDATES:
				return this.waitingUpdates;

			case BUCKETS_CHANGED_RATIO:
				measureValues = this.bucketsChangedRatio;
				value = 0.0;
				break;

			case CHANGED_AVG_BUCKETLENGTH_RATIO:
				measureValues = this.changedAvgBucketlengthRatios;
				value = 1.0;
				break;
			case CHANGED_MAX_BUCKETLENGTH_RATIO:
				measureValues = this.changedMaxBucketlengthRatios;
				value = 1.0;
				break;
			case CHANGED_BUCKETCOUNT_RATIO:
				measureValues = this.changedBucketcountRatios;
				value = 1.0;
				break;
			case WEIGHTED_CHANGERATE:
				measureValues = this.weightedChangerates;
				value = 1.0;
				break;
			case AVG_BUCKETLENGTH_CHANGERATE:
				measureValues = this.avgBucketlengthChangerates;
				value = 1.0;
				break;
			case MAX_BUCKETLENGTH_CHANGERATE:
				measureValues = this.maxBucketlengthChangerates;
				value = 1.0;
				break;
			default:
				return 0.0; // should not happen!
		}
		// if no info exists return standard value
		if (measureValues.size() == 0) {
			return value;
		}
		// otherwise calculate the average
		value = 0.0;
		for (Double currentValue : measureValues) {
			value += currentValue;
		}
		return value / measureValues.size();
	}

	/**
	 * method that returns the maximum value of the given measure.
	 * 
	 * @param measure the measure to retrieve
	 * @return the double value of the measure
	 */
	public double getMaxMeasure(Measure measure) {
		Vector<Double> measureValues;
		double value;
		switch (measure) {
			case WAITING_UPDATES:
				return this.waitingUpdates;

			case BUCKETS_CHANGED_RATIO:
				measureValues = this.bucketsChangedRatio;
				value = 0.0;
				break;

			case CHANGED_AVG_BUCKETLENGTH_RATIO:
				measureValues = this.changedAvgBucketlengthRatios;
				value = 1.0;
				break;
			case CHANGED_MAX_BUCKETLENGTH_RATIO:
				measureValues = this.changedMaxBucketlengthRatios;
				value = 1.0;
				break;
			case CHANGED_BUCKETCOUNT_RATIO:
				measureValues = this.changedBucketcountRatios;
				value = 1.0;
				break;
			case WEIGHTED_CHANGERATE:
				measureValues = this.weightedChangerates;
				value = 1.0;
				break;
			case AVG_BUCKETLENGTH_CHANGERATE:
				measureValues = this.avgBucketlengthChangerates;
				value = 1.0;
				break;
			case MAX_BUCKETLENGTH_CHANGERATE:
				measureValues = this.maxBucketlengthChangerates;
				value = 1.0;
				break;
			default:
				return 0.0; // should not happen!
		}
		// if no info exists return standard value
		if (measureValues.size() == 0) {
			return value;
		}
		// otherwise calculate the maximum
		for (Double currentValue : measureValues) {
			value = Math.max(value, currentValue);
		}
		return value;
	}

	/**
	 * @return returns bucketsChangedRatio
	 */
	public Vector<Double> getBucketsChangedRatio() {
		return this.bucketsChangedRatio;
	}

	/**
	 * @return returns changedAvgBucketlengthRatios
	 */
	public Vector<Double> getChangedAvgBucketlengthRatios() {
		return this.changedAvgBucketlengthRatios;
	}

	/**
	 * @return returns changedBucketcountRatios
	 */
	public Vector<Double> getChangedBucketcountRatios() {
		return this.changedBucketcountRatios;
	}

	/**
	 * @return returns changedMaxBucketlengthRatios
	 */
	public Vector<Double> getChangedMaxBucketlengthRatios() {
		return this.changedMaxBucketlengthRatios;
	}

	/**
	 * @return returns maxBucketlengthChangerates
	 */
	public Vector<Double> getMaxBucketlengthChangerates() {
		return this.maxBucketlengthChangerates;
	}

	
	/**
	 * @return returns nmbOfChangedBuckets
	 */
	public int getNmbOfChangedBuckets() {
		return this.nmbOfChangedBuckets;
	}

	/**
	 * @return returns waitingUpdates
	 */
	public int getWaitingUpdates() {
		return this.waitingUpdates;
	}

	/**
	 * @return returns weightedChangerates
	 */
	public Vector<Double> getWeightedChangerates() {
		return this.weightedChangerates;
	}

	/**
	 * method that tests if a threshold (of the given measures) for an index change was reached.
	 * 
	 * @param measures the measures (+ thresholds) to be used
	 * @return <tt>true</tt> if the index change should be propagated
	 */
	public boolean hasIndexChangedInRespectOfMeasures(HashMap<Measure, Double> measures) {
		// if no threshold should be used skip testing
		if (measures == null)
			return true;
		// test all given measures for a change
		for (Map.Entry<Measure, Double> entry : measures.entrySet()) {
			// if the current measure reached the threshold
			// alternative: if (this.getAvgMeasure(entry.getKey()) >= entry.getValue()) {
			if (this.getMaxMeasure(entry.getKey()) >= entry.getValue()) {
				// then it is time the propagate the changes
				return true;
			}
		}
		// if no measure reached the threshold then return false
		return false;
	}

	/**
	 * method that compares the current index change information with the given index change
	 * information. (used to decide whether to drop updates or not)
	 * 
	 * @param indexChangeInformation the information to compare to
	 * @return <tt>true</tt> if there are no important differences (in size and count)
	 */
	public boolean isEqual(IndexChangeInformation indexChangeInformation) {
		if ((this.getAvgMeasure(Measure.CHANGED_AVG_BUCKETLENGTH_RATIO) != indexChangeInformation.getAvgMeasure(Measure.CHANGED_AVG_BUCKETLENGTH_RATIO))
			|| (this.getAvgMeasure(Measure.CHANGED_BUCKETCOUNT_RATIO) != indexChangeInformation.getAvgMeasure(Measure.CHANGED_BUCKETCOUNT_RATIO))
			|| (this.getAvgMeasure(Measure.BUCKETS_CHANGED_RATIO) != indexChangeInformation.getAvgMeasure(Measure.BUCKETS_CHANGED_RATIO))) {
			return false;
		}
		return true;
	}

	/**
	 * @param avgBucketlengthChangerates sets avgBucketlengthChangerates
	 */
	public void setAvgBucketlengthChangerates(Vector<Double> avgBucketlengthChangerates) {
		this.avgBucketlengthChangerates = avgBucketlengthChangerates;
	}

	/**
	 * @param bucketsChangedRatio sets bucketsChangedRatio
	 */
	public void setBucketsChangedRatio(Vector<Double> bucketsChangedRatio) {
		this.bucketsChangedRatio = bucketsChangedRatio;
	}

	/**
	 * @param changedAvgBucketlengthRatios sets changedAvgBucketlengthRatios
	 */
	public void setChangedAvgBucketlengthRatios(Vector<Double> changedAvgBucketlengthRatios) {
		this.changedAvgBucketlengthRatios = changedAvgBucketlengthRatios;
	}

	/**
	 * @param changedBucketcountRatios sets changedBucketcountRatios
	 */
	public void setChangedBucketcountRatios(Vector<Double> changedBucketcountRatios) {
		this.changedBucketcountRatios = changedBucketcountRatios;
	}

	/**
	 * @param changedMaxBucketlengthRatios sets changedMaxBucketlengthRatios
	 */
	public void setChangedMaxBucketlengthRatios(Vector<Double> changedMaxBucketlengthRatios) {
		this.changedMaxBucketlengthRatios = changedMaxBucketlengthRatios;
	}

	/**
	 * @param maxBucketlengthChangerates sets maxBucketlengthChangerates
	 */
	public void setMaxBucketlengthChangerates(Vector<Double> maxBucketlengthChangerates) {
		this.maxBucketlengthChangerates = maxBucketlengthChangerates;
	}

	/**
	 * @param nmbOfChangedBuckets sets nmbOfChangedBuckets
	 */
	public void setNmbOfChangedBuckets(int nmbOfChangedBuckets) {
		this.nmbOfChangedBuckets = nmbOfChangedBuckets;
	}

	/**
	 * @param waitingUpdates sets waitingUpdates
	 */
	public void setWaitingUpdates(int waitingUpdates) {
		this.waitingUpdates = waitingUpdates;
	}

	/**
	 * @param weightedChangerates sets weightedChangerates
	 */
	public void setWeightedChangerates(Vector<Double> weightedChangerates) {
		this.weightedChangerates = weightedChangerates;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		Vector<Measure> measures = new Vector<Measure>();
		measures.add(Measure.CHANGED_AVG_BUCKETLENGTH_RATIO);
		measures.add(Measure.CHANGED_MAX_BUCKETLENGTH_RATIO);
		measures.add(Measure.CHANGED_BUCKETCOUNT_RATIO);
		measures.add(Measure.WEIGHTED_CHANGERATE);
		measures.add(Measure.BUCKETS_CHANGED_RATIO);
		measures.add(Measure.AVG_BUCKETLENGTH_CHANGERATE);
		measures.add(Measure.MAX_BUCKETLENGTH_CHANGERATE);
		measures.add(Measure.WAITING_UPDATES);
		return this.toString(measures);
	}

	/**
	 * method that returns the index change informationen (given measures) as String.
	 * 
	 * @param measures the measures to print
	 * @return the index change information (measures)
	 */
	@SuppressWarnings("nls")
	public String toString(Vector<Measure> measures) {
		StringBuffer sb = new StringBuffer();
		sb.append("IndexChangeInformation: (averaged)\n");
		// test all given measures for a change
		for (Measure measure : measures) {
			sb.append("\t");
			sb.append(measure.toString());
			sb.append(": ");
			sb.append(this.getAvgMeasure(measure));
			sb.append("\n");
		}
		return sb.toString();
	}

	/**
	 * method that computes a change rate based on the average bucket length between the new and the
	 * old index. (for more details see source)
	 * 
	 * @param bci the buckets change information 
	 * @return change rate
	 */
	private double getAverageBucketLengthChangeRate(BucketsChangeInformation bci) {
		// average the average bucket length over all buckets in the old index
		double averageBucketLengthInOldIndex;
		if (bci.getNmbOfBucketsInOldIndex() == 0.0) {
			averageBucketLengthInOldIndex = 0.0;
		} else {
			averageBucketLengthInOldIndex = bci.getTotalAverageBucketLengthInOldIndex()
												/ bci.getNmbOfBucketsInOldIndex();
		}
		// average the average bucket length over all buckets in the new index
		double averageBucketLengthInNewIndex;
		if (bci.getNmbOfBucketsInNewIndex() == 0.0) {
			averageBucketLengthInNewIndex = 0.0;
		} else {
			averageBucketLengthInNewIndex = bci.getTotalAverageBucketLengthInNewIndex()
												/ bci.getNmbOfBucketsInNewIndex();
		}
		// calculate the change rate with the following formula:
		//	max(length_new, length_old)
		//	---------------------------
		//  min(length_new, length_old)
		double minValue = Math.min(averageBucketLengthInNewIndex, averageBucketLengthInOldIndex);
		double maxValue = Math.max(averageBucketLengthInNewIndex, averageBucketLengthInOldIndex);
		if (minValue == 0.0) {
			if (maxValue == 0.0) {
				return 1.0; // no change
			}
			return maxValue;
		}
		return maxValue / minValue;
	}

	/**
	 * method that computes a change rate based on the maximum bucket length between the new and the
	 * old index. (for more details see source)
	 * 
	 * @param bci the buckets change information 
	 * @return change rate
	 */
	private double getMaximumBucketLengthChangeRate(BucketsChangeInformation bci) {
		// average the average bucket length over all buckets in the old index
		double averageBucketLengthInOldIndex;
		if (bci.getNmbOfBucketsInOldIndex() == 0.0) {
			averageBucketLengthInOldIndex = 0.0;
		} else {
			averageBucketLengthInOldIndex = bci.getTotalMaxBucketLengthInOldIndex()
												/ bci.getNmbOfBucketsInOldIndex();
		}
		// average the average bucket length over all buckets in the new index
		double averageBucketLengthInNewIndex;
		if (bci.getNmbOfBucketsInNewIndex() == 0.0) {
			averageBucketLengthInNewIndex = 0.0;
		} else {
			averageBucketLengthInNewIndex = bci.getTotalMaxBucketLengthInNewIndex()
												/ bci.getNmbOfBucketsInNewIndex();
		}
		// calculate the change rate with the following formula:
		//	max(length_new, length_old)
		//	---------------------------
		//  min(length_new, length_old)
		double minValue = Math.min(averageBucketLengthInNewIndex, averageBucketLengthInOldIndex);
		double maxValue = Math.max(averageBucketLengthInNewIndex, averageBucketLengthInOldIndex);
		if (minValue == 0.0) {
			if (maxValue == 0.0) {
				return 1.0; // no change
			}
			return maxValue;
		}
		return maxValue / minValue;
	}
}
