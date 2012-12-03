/*
 *
 */
package de.ilmenau.datasum.index.update;

import de.ilmenau.datasum.util.StringHelper;

/**
 * This class represents a bucket update.
 * 
 * @author Christian Lemke
 * @version $Id$
 */
@SuppressWarnings("serial")
public final class BucketIndexUpdateEntry extends IndexUpdateEntry {
	
	/** the bucket that is to be updated */
	private UpdateBucket bucket;


	/**
	 * constructor.
	 * 
	 * @param action action that should performed
	 * @param distToUpdatedPeer hop count distance to peer where this update originates from
	 * @param bucket the bucket that is to be updated
	 */
	public BucketIndexUpdateEntry(UpdateAction action, int distToUpdatedPeer, UpdateBucket bucket) {
		super(action, distToUpdatedPeer);
		
		this.bucket = bucket;
	}

	/**
	 * @return the bucket that is to be updated
	 */
	public UpdateBucket getBucket() {
		return this.bucket;
	}
	

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	@SuppressWarnings("nls")
	public String toString() {
		String returnString = "IndexUpdateEntry (";
		returnString += this.action + ", ";
		returnString += this.bucket.getGlobalBucketID() + ", ";
		returnString += this.bucket.getAttributeNames() + ", ";
		returnString += StringHelper.arrayToString(this.bucket.getLowerBoundaries()) + ", ";
		returnString += StringHelper.arrayToString(this.bucket.getUpperBoundaries()) + ", ";
		returnString += "distance " + this.distToUpdatedPeer;
		returnString += ")";
		return returnString;
	}
}
