/*
 *
 */
package de.ilmenau.datasum.index;

import de.ilmenau.datasum.util.bigmath.Space;

/**
 * This class represents a query space.
 * 
 * @author Christian Lemke
 * @version $Id$
 */
@SuppressWarnings("serial")
public final class QuerySpace extends Space {
	private static final long serialVersionUID = 1L;
	/** whether there are no restrictions to the query space */
	private boolean hasNoRestrictions;


	/**
	 * constructor.
	 * 
	 * @param lowerBoundaries the lower boundaries of the query space
	 * @param upperBoundaries the upper boundaries of the query space
	 * @param hasNoRestrictions whether there are no restrictions to the query space
	 */
	public QuerySpace(int[] lowerBoundaries, int[] upperBoundaries, boolean hasNoRestrictions) {
		super(lowerBoundaries, upperBoundaries);
		this.hasNoRestrictions = hasNoRestrictions;
	}

	/**
	 * @return returns whether there are no restrictions to the query space
	 */
	public boolean hasNoRestrictions() {
		return this.hasNoRestrictions;
	}
}
