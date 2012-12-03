/*
 *
 */
package de.ilmenau.datasum.util.serialization;

import java.io.Serializable;

/**
 * This class represents a serializable BucketBasedLocalIndex.
 * 
 * @author Christian Lemke
 * @version $Id$
 */
@SuppressWarnings("serial")
public final class SerialBucketIndex implements Serializable {
	private static final long serialVersionUID = 1L;
	/** number of indexed items in the index */
	private int nmbOfIndexedItems;
	/** number of buckets in the index */
	private int nmbOfBuckets;
	/** infos about index copies: number of copies */
	private int nmbOfIndexCopies;
	/** infos about index copies: number of buckets */
	private int nmbOfBucketsInIndexCopies;


	/**
	 * constructor.
	 */
	public SerialBucketIndex() {
		this.nmbOfIndexedItems = 0;
		this.nmbOfBuckets = 0;
		this.nmbOfIndexCopies = 0;
		this.nmbOfBucketsInIndexCopies = 0;
	}

	/**
	 * constructor.
	 * 
	 * @param nmbOfIndexedItems number of indexed items in the index
	 * @param nmbOfBuckets number of buckets in the index
	 * @param nmbOfIndexCopies nmbOfIndexCopies
	 * @param nmbOfBucketsInIndexCopies nmbOfBucketsInIndexCopies
	 */
	public SerialBucketIndex(int nmbOfIndexedItems, int nmbOfBuckets, int nmbOfIndexCopies,
			int nmbOfBucketsInIndexCopies) {
		this.nmbOfIndexedItems = nmbOfIndexedItems;
		this.nmbOfBuckets = nmbOfBuckets;
		this.nmbOfIndexCopies = nmbOfIndexCopies;
		this.nmbOfBucketsInIndexCopies = nmbOfBucketsInIndexCopies;
	}

	/**
	 * @return returns nmbOfBuckets
	 */
	public int getNmbOfBuckets() {
		return this.nmbOfBuckets;
	}

	/**
	 * @return returns nmbOfBucketsInIndexCopies
	 */
	public int getNmbOfBucketsInIndexCopies() {
		return this.nmbOfBucketsInIndexCopies;
	}

	/**
	 * @return returns nmbOfIndexCopies
	 */
	public int getNmbOfIndexCopies() {
		return this.nmbOfIndexCopies;
	}

	/**
	 * @return returns nmbOfIndexedItems
	 */
	public int getNmbOfIndexedItems() {
		return this.nmbOfIndexedItems;
	}

	/**
	 * @param nmbOfBuckets sets nmbOfBuckets
	 */
	public void setNmbOfBuckets(int nmbOfBuckets) {
		this.nmbOfBuckets = nmbOfBuckets;
	}

	/**
	 * @param nmbOfBucketsInIndexCopies sets nmbOfBucketsInIndexCopies
	 */
	public void setNmbOfBucketsInIndexCopies(int nmbOfBucketsInIndexCopies) {
		this.nmbOfBucketsInIndexCopies = nmbOfBucketsInIndexCopies;
	}

	/**
	 * @param nmbOfIndexCopies sets nmbOfIndexCopies
	 */
	public void setNmbOfIndexCopies(int nmbOfIndexCopies) {
		this.nmbOfIndexCopies = nmbOfIndexCopies;
	}

	/**
	 * @param nmbOfIndexedItems sets nmbOfIndexedItems
	 */
	public void setNmbOfIndexedItems(int nmbOfIndexedItems) {
		this.nmbOfIndexedItems = nmbOfIndexedItems;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@SuppressWarnings("nls")
	@Override
	public String toString() {
		return "Index:\n\t#indexed items: " + this.nmbOfIndexedItems
			+ "\n\t#buckets: " + this.nmbOfBuckets + "\n\t#index copies: "
			+ this.nmbOfIndexCopies + "\n\t#buckets of copies: "
			+ this.nmbOfBucketsInIndexCopies + "\n";
	}
}
