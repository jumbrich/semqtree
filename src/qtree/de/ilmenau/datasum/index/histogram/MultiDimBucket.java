package de.ilmenau.datasum.index.histogram;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

import de.ilmenau.datasum.index.Bucket;

/**
 * This class represents a multidimensional histogram bucket.
 * 
 * @author Katja Hose
 * @author Christian Lemke
 * @version $Id$
 */
@SuppressWarnings("serial")
public class MultiDimBucket extends Bucket {

	/** a reference to the index */
	private MultiDimHistogram correspondingIndex;


	/**
	 * Creates a new Bucket object.
	 * 
	 * @param min minimum boundaries for each dimension
	 * @param max maximum boundaries for each dimension
	 * @param count the initial count of elements
	 * @param correspondingIndex a reference to the index
	 * @param peerID the ID of the peer where the corresponding index is located
	 * @param neighborID the ID of the peer where the corresponding index is located
	 * @param localBucketID local ID for identifying buckets in one index
	 */
	public MultiDimBucket(int[] min, int[] max, double count, MultiDimHistogram correspondingIndex,
			String peerID, String neighborID, long localBucketID, HashSet<String> sourceIDs) {
		super(min, max, count, peerID, neighborID, localBucketID, sourceIDs);
		this.correspondingIndex = correspondingIndex;
		this.correspondingIndex.updateNmbOfItemsInIndex(count);
	}
	
	
	/**
	 * Creates a new Bucket object.
	 * 
	 * @param min minimum boundaries for each dimension
	 * @param max maximum boundaries for each dimension
	 * @param count the initial count of elements
	 * @param correspondingIndex a reference to the index
	 * @param peerID the ID of the peer where the corresponding index is located
	 * @param neighborID the ID of the peer where the corresponding index is located
	 * @param localBucketID local ID for identifying buckets in one index
	 */
	public MultiDimBucket(int[] min, int[] max, double count, MultiDimHistogram correspondingIndex,
			String peerID, String neighborID, long localBucketID, HashMap<String,Double> sourceIDCounts) {
		super(min, max, count, peerID, neighborID, localBucketID, sourceIDCounts);
		this.correspondingIndex = correspondingIndex;
		this.correspondingIndex.updateNmbOfItemsInIndex(count);
	}
	
	
	/* (non-Javadoc)
	 * @see smurfpdms.index.Bucket#getAttributeNames()
	 */
	@Override
	public Vector<String> getAttributeNames() {
		return this.correspondingIndex.getAttributeNames();
	}

	/* (non-Javadoc)
	 * @see smurfpdms.index.Bucket#updateCount(double)
	 */
	@SuppressWarnings("nls")
	@Override
	public void updateCount(double amount) {
		if (this.count + amount < -0.5) {
			throw new IllegalStateException("Not enough data items in the bucket after update!");
		}
		// update the number of elements in the bucket and index
		this.count += amount;
		this.correspondingIndex.updateNmbOfItemsInIndex(amount);
		// remove if empty
		if (this.count < 0.5) {
			this.correspondingIndex.updateNmbOfItemsInIndex(this.count * -1.0);
			this.correspondingIndex.removeBucket((int) this.getLocalBucketID());
		}
	}
	
	/* (non-Javadoc)
	 * @see smurfpdms.index.Bucket#updateCount(double)
	 */
	@Override
	public void updateCount(double amount, String sourceID) {
		
		if (this.correspondingIndex.storeDetailedCounts()){
			
			Double count = this.sourceIDCountMap.get(sourceID);
			if (count == null){
				this.sourceIDCountMap.put(sourceID, new Double(amount));
			} else {
				this.sourceIDCountMap.put(sourceID, new Double(count.doubleValue()+amount));
			}
			
		} else {
			this.addSourceID(sourceID);
		}
		
		this.updateCount(amount);
	}
	
	
	/**
	 * @param correspondingIndex sets correspondingIndex
	 */
	void setCorrespondingIndex(MultiDimHistogram correspondingIndex) {
		this.correspondingIndex = correspondingIndex;
	}
}
