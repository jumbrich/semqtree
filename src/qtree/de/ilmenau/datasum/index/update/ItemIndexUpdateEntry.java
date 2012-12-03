/*
 *
 */
package de.ilmenau.datasum.index.update;

import de.ilmenau.datasum.util.StringHelper;

/**
 * This class represents an item update.
 * 
 * @author Christian Lemke
 * @version $Id$
 */
@SuppressWarnings({"serial", "nls"})
public final class ItemIndexUpdateEntry extends IndexUpdateEntry {

	/** coordinates (attribute values) of the data item that is to be updated */
	private int[] coordinates;
	/** names of the attributes to change --> in the same order as in coordinates */
	private String[] attributeNames;


	/**
	 * constructor.
	 * 
	 * @param action action that should performed
	 * @param attributeNames names of the attributes to change
	 * @param coordinatesOfChange attribute values
	 * @param distToUpdatedPeer hop count distance to peer where this update originates from
	 */
	public ItemIndexUpdateEntry(UpdateAction action, String[] attributeNames, int[] coordinatesOfChange, int distToUpdatedPeer) {
		super(action, distToUpdatedPeer);

		this.attributeNames = attributeNames;
		this.coordinates = coordinatesOfChange;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		ItemIndexUpdateEntry clone = (ItemIndexUpdateEntry) super.clone();

		// do a deep copy
		if (this.attributeNames != null) {
			clone.attributeNames = this.attributeNames.clone();
		}

		return clone;
	}

	/**
	 * @return the attribute values
	 */
	public int[] getCoordinates() {
		return this.coordinates;
	}
	
	
	/**
	 * @return the attribute names
	 */
	public String[] getAttributeNames() {
		return this.attributeNames;
	}
	

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	@SuppressWarnings("nls")
	public String toString() {
		String returnString = "IndexUpdateEntry (";
		returnString += this.action + ", ";
		returnString += StringHelper.arrayToString(this.attributeNames) + ", ";
		returnString += StringHelper.arrayToString(this.coordinates) + ", ";
		returnString += "distance " + this.distToUpdatedPeer;
		returnString += ")";
		return returnString;
	}
}
