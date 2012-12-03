/*
 *
 */
package de.ilmenau.datasum.index;

/**
 * This class represents a Bucket based CompoundRoutingIndex.
 * 
 * @author Christian Lemke
 * @version $Id$
 */
public abstract class BucketBasedCompoundRoutingIndex extends CompoundRoutingIndex {
	private static final long serialVersionUID = 1L;
	/**
	 * constructor.
	 * 
	 * @param type the index type
	 */
	public BucketBasedCompoundRoutingIndex(IndexType type) {
		super(type);
	}
}
