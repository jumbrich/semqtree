/*
 *
 */
package de.ilmenau.datasum.index.update;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

import de.ilmenau.datasum.util.DataPoint;
import de.ilmenau.datasum.util.bigmath.Space;


/**
 * This class represents a lightweigth bucket for updates.
 * 
 * @author Christian Lemke
 * @version $Id$
 */
@SuppressWarnings("serial")
public class UpdateBucket extends Space implements Serializable {

	/** name of the indexed attributes */
	private Vector<String> attributeNames;
	/** the number of elements in the bucket */
	private double count;

	/** the ID of the peer where the corresponding index is located */
	private int peerID;
	/** the ID of the neighbor (<tt>null</tt> if the bucket is not part of a routing index) */
	private int neighborID;
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

	/**
	 * defines which data points to remove at the receiver of this update bucket
	 * (used for evaluation and {@link IndexUpdateEntry.UpdateAction#UPD_BUCK}) */
	private Vector<DataPoint> pointsToRemove;
	
	
	private HashSet<String> sourceIDs = new HashSet<String>();
	
	private HashMap<String,Double> sourceIDMap = new HashMap<String,Double>();


	/**
	 * constructor.
	 * 
	 * @param lowerBoundaries the lower boundaries
	 * @param upperBoundaries the upper boundaries
	 * @param count the number of elements in the bucket
	 * @param attributeNames the names of the indexed attributes 
	 * @param peerID the ID of the peer where the corresponding index is located
	 * @param neighborID the ID of the peer where the corresponding index is located
	 * @param localBucketID local ID for identifying buckets in one index
	 */
	public UpdateBucket(int[] lowerBoundaries, int[] upperBoundaries, double count, Vector<String> attributeNames,
			int peerID, int neighborID, long localBucketID) {
		super(lowerBoundaries, upperBoundaries);
		this.attributeNames = attributeNames;
		this.count = count;
		this.peerID = peerID;
		this.neighborID = neighborID;
		this.localBucketID = localBucketID;
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
	
	
	public HashSet<String> getSourceIDs(){
		return this.sourceIDs;		
	}
	
	public void setSourceIDs(HashSet<String> sources){
		this.sourceIDs = sources;
	}
	
	public void setSourceIDMap(HashMap<String,Double> sourceIDMap){
		this.sourceIDMap = sourceIDMap;
	}
	
	public HashMap<String,Double> getSourceIDMap(){
		return this.sourceIDMap;
	}

	/**
	 * Returns the names of the attributes (for each dimension).
	 * 
	 * @return the names of the attributes
	 */
	public Vector<String> getAttributeNames() {
		return this.attributeNames;
	}

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
			this.globalBucketIDCache = this.peerID + "-" + this.getNeighborID() + "-" + this.localBucketID;
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
		if (this.neighborID == -1) {
			return null;
		}
		return Integer.toString(this.neighborID);
	}

	/**
	 * @return returns peerID
	 */
	public String getPeerID() {
		if (this.peerID == -1) {
			return null;
		}
		return Integer.toString(this.peerID);
	}

	
	/**
	 * @return returns the data points to remove (only used for evaluation)
	 */
	public Vector<DataPoint> getPointsToRemove() {
		return this.pointsToRemove;
	}

	
	/**
	 * @param pointsToRemove sets the data points to remove (only used for evaluation)
	 */
	public void setPointsToRemove(Vector<DataPoint> pointsToRemove) {
		this.pointsToRemove = pointsToRemove;
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
		sb.append(this.getGlobalBucketID());
		sb.append("]  count=");
		sb.append(this.count);
		sb.append("  space=");
		sb.append(super.toString());
		sb.append("  sources ("+sourceIDs.size()+") ="+sourceIDs.toString());

		return sb.toString();
	}
}
