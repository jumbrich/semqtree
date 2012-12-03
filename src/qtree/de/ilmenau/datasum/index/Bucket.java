/*
 * 
 */
package de.ilmenau.datasum.index;



import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;
import java.util.Map.Entry;

import de.ilmenau.datasum.config.Parameters;
import de.ilmenau.datasum.index.update.UpdateBucket;
import de.ilmenau.datasum.util.DataPoint;
import de.ilmenau.datasum.util.bigmath.Space;
import de.ilmenau.datasum.util.serialization.SerialBucket;

/**
 * This class represents a bucket in a histogram or QTree.
 * 
 * @author Christian Lemke
 * @version $Id$
 */
public abstract class Bucket extends Space implements Cloneable, Serializable {
	private static final long serialVersionUID = 1L;
	/** the number of elements in the bucket */
	protected double count;

	/** the ID of the peer where the corresponding index is located */
	private String peerID;
	/** the ID of the neighbor (<tt>null</tt> if the bucket is not part of a routing index) */
	private String neighborID;
	/** local ID for identifying buckets in one index */
	private long localBucketID;
	/**
	 * String for identifying buckets not only in just one index but globally.
	 * It is built as follows: <tt>globalBucketID = peerID + "-" + neighborID +"-" + localBucketID</tt>.
	 * As long as one peer has only one Index for describing its local data the globalBucketID is
	 * unique for the corresponding CRI.
	 * In case more than one local Index is allowed we for example have to add the indexed
	 * dimensions or any other kind of additional ID that makes it globally unique!
	 *  
	 * This ID is needed to be able to compare buckets while computing change rates
	 */
	private transient String globalBucketIDCache = null;
	
	private HashSet<String> sourceIDs = new HashSet<String>();
	
	public HashMap<String,Double> sourceIDCountMap = new HashMap<String, Double>();


	/**
	 * standard constructor.
	 */
	public Bucket() {
		super(null, null);
		this.count = 0.0;
		this.peerID = null;
		this.neighborID = null;
		this.localBucketID = -1;
	}
	
	/**
	 * constructor. Necessary because inner QTree nodes extend this class, too.
	 * 
	 * @param lowerBoundaries the lower boundaries
	 * @param upperBoundaries the upper boundaries
	 * @param count the number of elements in the bucket
	 * @param peerID the ID of the peer where the corresponding index is located
	 * @param neighborID the ID of the peer where the corresponding index is located
	 */
	public Bucket(int[] lowerBoundaries, int[] upperBoundaries, double count, String peerID, String neighborID, String sourceID) {
		super(lowerBoundaries, upperBoundaries);
		this.count = count;
		this.peerID = peerID;
		this.neighborID = neighborID;
		if (sourceID != null){
			sourceIDs.add(sourceID);
		}
	}

	/**
	 * constructor.
	 * 
	 * @param lowerBoundaries the lower boundaries
	 * @param upperBoundaries the upper boundaries
	 * @param count the number of elements in the bucket
	 * @param peerID the ID of the peer where the corresponding index is located
	 * @param neighborID the ID of the peer where the corresponding index is located
	 * @param localBucketID local ID for identifying buckets in one index
	 */
	public Bucket(int[] lowerBoundaries, int[] upperBoundaries, double count, String peerID, String neighborID,
			long localBucketID, HashSet<String> sourceIDs) {
		super(lowerBoundaries, upperBoundaries);
		this.count = count;
		this.peerID = peerID;
		this.neighborID = neighborID;
		this.localBucketID = localBucketID;
		addSourceIDs(sourceIDs);
	}
	
	/**
	 * constructor.
	 * 
	 * @param lowerBoundaries the lower boundaries
	 * @param upperBoundaries the upper boundaries
	 * @param count the number of elements in the bucket
	 * @param peerID the ID of the peer where the corresponding index is located
	 * @param neighborID the ID of the peer where the corresponding index is located
	 * @param localBucketID local ID for identifying buckets in one index
	 */
	public Bucket(int[] lowerBoundaries, int[] upperBoundaries, double count, String peerID, String neighborID,
			long localBucketID, HashMap<String,Double> sourceIDMap) {
		super(lowerBoundaries, upperBoundaries);
		
		// added by MKa to have the global count updated as well
		this.count = count;
//		this.count = 0.0;
//		for (Double c : sourceIDMap.values()) this.count += c;
		
		this.peerID = peerID;
		this.neighborID = neighborID;
		this.localBucketID = localBucketID;
		this.sourceIDCountMap = sourceIDMap;
	}
	
	
	/**
	 * constructor.
	 * 
	 * @param lowerBoundaries the lower boundaries
	 * @param upperBoundaries the upper boundaries
	 * @param count the number of elements in the bucket
	 * @param peerID the ID of the peer where the corresponding index is located
	 * @param neighborID the ID of the peer where the corresponding index is located
	 * @param localBucketID local ID for identifying buckets in one index
	 */
	public Bucket(int[] lowerBoundaries, int[] upperBoundaries, String peerID, String neighborID,
			long localBucketID, HashMap<String,Double> sourceCounts) {
		super(lowerBoundaries, upperBoundaries);
		
		// added by MKa to have the global count updated as well
//		this.count = count;
		this.count = 0.0;
		for (Double c : sourceCounts.values()) this.count += c;

		this.peerID = peerID;
		this.neighborID = neighborID;
		this.localBucketID = localBucketID;
		
		this.sourceIDCountMap.putAll(sourceCounts);
	}
	
	
	public void addSourceID(String source){
		if (!sourceIDs.contains(source)){
			this.sourceIDs.add(source);
		}
	}
	
	
	public void addSourceID(String source, boolean storeDetailedCounts){
		//System.out.println("Bucket: "+source);
		
		if (!storeDetailedCounts){
			if (!sourceIDs.contains(source)){
				this.sourceIDs.add(source);
			}
		} else {
			// added by MKa to have the global count updated as well
			this.count += 1.0;
			
			Double count = this.sourceIDCountMap.get(source);
			if (count == null){
				this.sourceIDCountMap.put(source, new Double(1.0));
			} else {
				this.sourceIDCountMap.put(source, new Double(count.doubleValue()+1.0));
			}
		}
		//System.out.println("Bucket.sourceIDs"+sourceIDs.toString());
	}
	
	public void addSourceIDs(HashSet<String> sourceIDs){
		this.sourceIDs.addAll(sourceIDs);
	}
	
	public void addSourceIDsAndCount(HashMap<String, Double> sourceIDMap){
		for (Entry<String,Double> ent: sourceIDMap.entrySet()){
			// added by MKa to have the global count updated as well
			this.count += ent.getValue();
			
			Double act = this.sourceIDCountMap.get(ent.getKey());
			if (act == null){
				this.sourceIDCountMap.put(ent.getKey(),ent.getValue());
			} else {
				this.sourceIDCountMap.put(ent.getKey(),new Double(act+ent.getValue()));
			}
		}
		
	}	
	
	
	public HashSet<String> getSourceIDs(){
		return this.sourceIDs;
	}
	
	public HashMap<String,Double> getSourceIDMap(){
		return this.sourceIDCountMap;
	}
	

	/* (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		try {
			return super.clone();
		} catch (CloneNotSupportedException e) {
			// can't happen; we implement Cloneable
			return null;
		}
	}

	/**
	 * method that creates a lightweight bucket for updates.
	 * (No direct/indirect references to the index object are allowed!)
	 * 
	 * @return a lightweight bucket for updates
	 */
	public UpdateBucket createUpdateBucket() {
		int integerPeerID = -1;
		if (this.peerID != null) {
			try {
				integerPeerID = Integer.parseInt(this.peerID);
			} catch (NumberFormatException nfe) {
				// do nothing
			}
		}
		int integerNeighborID = -1;
		if (this.peerID != null) {
			try {
				integerNeighborID = Integer.parseInt(this.neighborID);
			} catch (NumberFormatException nfe) {
				// do nothing
			}
		} 

		UpdateBucket updateBucket = new UpdateBucket(
			this.lowerBoundaries, this.upperBoundaries, this.count, this.getAttributeNames(),
			integerPeerID, integerNeighborID, this.localBucketID
		);
		// only for evaluation forward the points in the bucket!
		if (Parameters.storeDataPointsInBuckets) {
			updateBucket.addPoints(this.getContainedPoints());
		}
		return updateBucket;
	}

	/**
	 * Returns the names of the attributes (for each dimension).
	 * 
	 * @return the names of the attributes
	 */
	public abstract Vector<String> getAttributeNames();

	/**
	 * @return returns the number of elements in the bucket
	 */
	public double getCount() {
		return this.count;
	}

	/**
	 * @return a bucketID as String that is globally unique
	 */
	@SuppressWarnings("nls")
	public String getGlobalBucketID() {
		if (this.globalBucketIDCache == null) {
			this.globalBucketIDCache = this.peerID + "-" + this.neighborID + "-" + this.localBucketID;
		}
		return this.globalBucketIDCache;
	}

	/**
	 * @return a bucketID as long that is unique for the index that this bucket belongs to
	 */
	public long getLocalBucketID() {
		return this.localBucketID;
	}

	/**
	 * @return returns neighborID
	 */
	public String getNeighborID() {
		return this.neighborID;
	}

	/**
	 * @return returns peerID
	 */
	public String getPeerID() {
		return this.peerID;
	}

	/**
	 * method that returns a serializable Bucket for java.beans.XMLEncoder.
	 * Overwrite this method in sub classes for other SerialBuckets.
	 * 
	 * @return SerialBucket
	 */
	public SerialBucket getSerializableBucket() {
		Vector<int[]> points = null;
		// evaluation?
		Vector<DataPoint> dataPoints = this.getContainedPoints();
		if (dataPoints != null && !dataPoints.isEmpty()) {
			// serialize also the contained data points
			points = new Vector<int[]>();
			for (DataPoint p : dataPoints) {
				points.add(p.getCoordinates());
			}
		}

		return new SerialBucket(
			this.lowerBoundaries, this.upperBoundaries, this.count, points
		);
	}

	/**
	 * method that tests if the given constraints match to this bucket.
	 * (used by function hasAccordingData in the class MutiDimHistogram)
	 * 
	 * @param constraints the constraints to test
	 * @return <tt>true</tt> if all constraints match to this bucket
	 */
	public boolean matchConstraints(Vector<SelectConstraint> constraints) {
		if (constraints.isEmpty()) return true;
		// test the constraints
		// --> abort and return false if one constraint do not match
		for (SelectConstraint constraint : constraints) {
			int index = constraint.getElementIndex();
			int value = constraint.getElementValue();
			switch (constraint.getComparisonOperator()) {
				case SMALLER:
					if (value <= this.lowerBoundaries[index]) {
						return false;
					}
					break;
				case SMALLER_OR_EQUAL:
					if (value < this.lowerBoundaries[index]) {
						return false;
					}
					break;
				case EQUAL:
					if (value < this.lowerBoundaries[index] || value > this.upperBoundaries[index]) {
						return false;
					}
					break;
				case GREATER_OR_EQUAL:
					if (value > this.upperBoundaries[index]) {
						return false;
					}
					break;
				case GREATER:
					if (value >= this.upperBoundaries[index]) {
						return false;
					}
					break;
			}
		}
		return true;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@SuppressWarnings("nls")
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();

		sb.append(this.getClass().getSimpleName());
		sb.append(":  ID=[");
		sb.append("]  count=");
		sb.append(this.count);
		/*sb.append(this.getGlobalBucketID());
		sb.append("]  ");
		if (this.sourceIDCountMap.isEmpty()){
			sb.append("count=");
			sb.append(this.count);
		} else {
			sb.append("sourceIDsCountMap=");
			sb.append(this.sourceIDCountMap.toString());
		}*/
		sb.append("  space=");
		sb.append(super.toString());

		return sb.toString();
	}

	/**
	 * abstract method that updates the number of elements in this bucket.
	 * (if count is less than 0.5 the bucket will be removed!)
	 * 
	 * @param amount how much to add (positive) or remove (negative)
	 */
	public abstract void updateCount(double amount);
	
	public abstract void updateCount(double amount, String sourceID);
	
	
	/**
	 * 
	 * @param newID
	 */
	public void setLocalBucketID(long newID){
		this.localBucketID = newID;
	}
	
	/**
	 * 
	 * @param newID
	 */
	public void setGlobalBucketID(String newID){
		this.globalBucketIDCache = newID;
	}
}