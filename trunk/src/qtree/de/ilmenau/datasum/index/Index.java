package de.ilmenau.datasum.index;

import java.io.Serializable;

/**
 * This class represents an Index.
 * 
 * @author Katja Hose
 * @version 1.0
 * \date    created on 30.06.2004
 * \bug no bugs reported 
 * \warning therefore no warnings
 * 
 */
public abstract class Index implements Serializable, Cloneable {
	private static final long serialVersionUID = 1L;
	/** the index types */
	public enum IndexType {
		/** smurfpdms.index.xpath.XPathIndex */
		PathCRI,
		/** smurfpdms.index.xpath.XPathIndex */
		ElementCRI,
		/** smurfpdms.index.qtree.QTree */
		QTree,
		/** smurfpdms.index.histogram.MultiDimHistogram */
		MultiDimHistogram;
	}
	

	/** represents the kind of index that this object represents */
	protected IndexType type;


	/**
	 * standard constructor.
	 * 
	 * @param type enum that represents the kind of index that the instantiated object represents
	 */
	public Index(IndexType type) {
		this.type = type;
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
	 * abstract method for getting the state string of the Index object.
	 * 
	 * @return state string 
	 */
	public abstract String getStateString();

	/**
	 * method for getting the type value of an Index object.
	 * 
	 * @return kind of index
	 */
	public IndexType getType() {
		return this.type;
	}

	/**
	 * abstract method for determining if this index object is empty or not 
	 * 
	 * @return <tt>true</tt> if index has no data
	 */
	public abstract boolean isEmpty();
}
