package de.ilmenau.datasum.index.update;

import java.io.Serializable;

/**
 * This class represents an update for an index.
 * 
 * @author Katja Hose
 * @version $Id$
 */
@SuppressWarnings({"serial"})
public abstract class IndexUpdateEntry implements Serializable, Cloneable {

	/** 
	 * action that should performed
	 * (To get a compareable size for an update entry the name of each enum must have the same length!)
	 */
	public enum UpdateAction {
		/** add a data item */
		ADD_ITEM,
		/** remove a data item */
		RMV_ITEM,
		/** add a bucket */
		ADD_BUCK,
		/** update a bucket */
		UPD_BUCK,
		/** remove a bucket */
		RMV_BUCK
	}

	/** keyword of the action that is to be performed */
	protected UpdateAction action;
	/** hop count distance to peer where this update originates from */
	protected int distToUpdatedPeer;


	/**
	 * constructor.
	 * 
	 * @param action action that should performed
	 * @param distToUpdatedPeer hop count distance to peer where this update originates from
	 */
	public IndexUpdateEntry(UpdateAction action, int distToUpdatedPeer) {
		super();

		this.action = action;
		this.distToUpdatedPeer = distToUpdatedPeer;
	}

	/**
	 * @return action that should performed
	 */
	public UpdateAction getAction() {
		return this.action;
	}

	/**
	 * @return hop count distance to peer where this update originates from
	 */
	public int getDistance() {
		return this.distToUpdatedPeer;
	}

	/**
	 * method that increments the current hop count of this update
	 */
	public void increaseDistance() {
		this.distToUpdatedPeer++;
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
}
