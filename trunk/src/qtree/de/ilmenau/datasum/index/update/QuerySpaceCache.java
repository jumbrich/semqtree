/*
 * Created: 02.02.2007
 * Changed: $Date$
 * 
 * $Log$
 * Revision 1.5  2007-04-30 15:34:02  lemke
 * + small fix
 *
 * Revision 1.4  2007/04/05 16:49:00  lemke
 * + small calculation change
 *
 * Revision 1.3  2007/02/05 16:04:52  lemke
 * + added parameter for the update strategy QUERY_FEEDBACK to the event config
 *
 * Revision 1.2  2007/02/05 11:07:00  lemke
 * + improved the function isCoveredByCache()
 *
 * Revision 1.1  2007/02/03 19:48:55  lemke
 * + initial import
 *
 */
package de.ilmenau.datasum.index.update;


import java.util.Iterator;
import java.util.Vector;

import de.ilmenau.datasum.config.Parameters;
import de.ilmenau.datasum.index.QuerySpace;
import de.ilmenau.datasum.util.bigmath.BigMathHelper;
import de.ilmenau.datasum.util.bigmath.BigUInt;
import de.ilmenau.datasum.util.bigmath.DifferentByteSizeException;
import de.ilmenau.datasum.util.bigmath.Space;

/**
 * This class represents a cache for old query spaces.
 * 
 * @author Christian Lemke
 * @version $Id$
 */
public final class QuerySpaceCache {

	/**
	 * This class represents a cache entry.
	 * 
	 * @author Christian Lemke
	 * @version $Id$
	 */
	private class CacheEntry {

		/** the timestamp when the query (answer) was received */
		public int timeStep;
		/** the query space */
		public QuerySpace querySpace;

		/**
		 * constructor.
		 * 
		 * @param timeStep the timestamp when the query (answer) was received
		 * @param qs the query space
		 */
		public CacheEntry(int timeStep, QuerySpace qs) {
			this.timeStep = timeStep;
			this.querySpace = qs;
		}
	}


	/** the maximum number of entries in the cache */
	private int maximumEntries;
	/** the maximum age of an cache entry */
	private int maximumAge;
	/** the cache */
	private Vector<CacheEntry> entries;

	/**
	 * constructor.
	 * 
	 * @param maximumEntries the maximum number of entries in the cache
	 * @param maximumAge the maximum age of an cache entry
	 */
	public QuerySpaceCache(int maximumEntries, int maximumAge) {
		this.maximumEntries = maximumEntries;
		this.maximumAge = maximumAge;
		this.entries = new Vector<CacheEntry>();
	}

	/**
	 * method that adds a new query space to the cache and cleanups old entries.
	 * 
	 * @param querySpace the query space to add
	 * @param currentTimeStep the current time step (when the query was received)
	 */
	public void add(QuerySpace querySpace, int currentTimeStep) {
		if (querySpace.hasNoRestrictions()) {
			// if the query space is the complete data space then remove the old entries
			this.entries.clear();
		} else {
			Iterator<CacheEntry> it = this.entries.iterator();
			CacheEntry oldestEntry = null;
			// before adding the new query space test the existing
			while (it.hasNext()) {
				CacheEntry entry = it.next();
				// remove outdated cache entries
				if (currentTimeStep - entry.timeStep > this.maximumAge - 1) {
					it.remove();
					continue;
				}
				// remove query spaces that are in the new query space
				if (querySpace.contains(entry.querySpace)) {
					it.remove();
					continue;
				}
				// determine the oldest entry
				if (oldestEntry == null) {
					oldestEntry = entry;
				} else {
					if (entry.timeStep < oldestEntry.timeStep) {
						oldestEntry = entry;
					}
				}
			}
			// if there is still no room then remove the oldest entry
			if (this.entries.size() == this.maximumEntries) {
				this.entries.remove(oldestEntry);
			}
		}
		// at last add the new entry
		this.entries.add(new CacheEntry(currentTimeStep, querySpace));
	}

	/**
	 * method that determines whether the given query space is covered by old query spaces in the
	 * cache.
	 * 
	 * @param qs the query space
	 * @param currentTimeStep the current simulation step
	 * @return <tt>true</tt> if there are up-to-date index information for the given query space 
	 */
	public boolean isCoveredByCache(QuerySpace qs, int currentTimeStep) {
		// first remove outdated cache entries
		// and find query space that is unrestricted (no constraints)
		boolean unrestrictedSpaceExists = false;
		Iterator<CacheEntry> it = this.entries.iterator();
		while (it.hasNext()) {
			CacheEntry entry = it.next();
			if (currentTimeStep - entry.timeStep > this.maximumAge) {
				it.remove();
			} else {
				// if not outdated then test if restricted
				if (entry.querySpace.hasNoRestrictions()) {
					unrestrictedSpaceExists = true;
				}
			}
		}
		// abort if the cache is (now) empty
		if (this.entries.isEmpty()) {
			return false;
		}
		// stop calculation if there is an unrestricted query space in the cache
		if (unrestrictedSpaceExists) {
			return true;
		}
		// now test if the current (given) query space is covered by the cache entries
		// --> first get the intersections with the given query space
		Vector<Space> baseIntersections = new Vector<Space>();
		for (CacheEntry entry : this.entries) {
			Space intersection = entry.querySpace.intersect(qs);
			if (intersection != null) {
				baseIntersections.add(intersection);
			}
		}
		// --> calculate the space for the intermediary results and the final result (BigUInt)
		int estimatedByteSizeNeeded = BigMathHelper.getByteSizeToUse(
			BigMathHelper.getEstimatedCapacityBitSize(qs)
		);
		BigUInt totalCapacity = new BigUInt(estimatedByteSizeNeeded);
		BigUInt partialCapacity = new BigUInt(estimatedByteSizeNeeded);
		// --> now iterate over the base intersetions and determine the covered capacity
		for (int i = 0; i < baseIntersections.size(); i++) {
			Space intersection = baseIntersections.get(i);
			intersection.calculateCapacity(partialCapacity);
			try {
				totalCapacity.add(partialCapacity);
			} catch (DifferentByteSizeException e) {
				// could not happen
			}
			// calculate the sieve formula recursive
			sieveFormulaRecursion(
				true, intersection, baseIntersections, i + 1, partialCapacity, totalCapacity
			);
		}
		// --> then calculate the total capacity
		BigUInt querySpaceCapacity = new BigUInt(estimatedByteSizeNeeded);
		qs.calculateCapacity(querySpaceCapacity);
		// --> at last calculate the ratio (covered capacity / total capacity)
		try {
			partialCapacity.getValueFrom(totalCapacity);
			// 'FIXPOINT_RANGE_EXTENSION * 8' bits dual floating points
			partialCapacity.shiftLeft(BigMathHelper.FIXPOINT_RANGE_EXTENSION * 8);
			partialCapacity.div(querySpaceCapacity);
		} catch (DifferentByteSizeException e) {
			// could not happen
		}
		// to double
		double ratio = partialCapacity.getLower63Bits()
							/ Math.pow(2.0, BigMathHelper.FIXPOINT_RANGE_EXTENSION * 8);
		// return whether the query space is covered by the cache entries 
		if (ratio >= Parameters.minCoveredRatioInQuerySpaceCache) {
			return true;
		}
		return false;
	}

	/**
	 * method that is used to calculate the sieve formula recursive.
	 * 
	 * @param minusSign whether to subtract/add the current capacity
	 * @param lastIntersection the intersection from the last recursion step
	 * @param baseIntersections the base intersections
	 * @param nextBaseIntersectionNumber which base intersection was used in the last recursion step
	 * @param partialCapacity to store the current capacity (temporary variable)
	 * @param totalCapacity the total capacity (till now)
	 */
	private void sieveFormulaRecursion(boolean minusSign, Space lastIntersection,
			Vector<Space> baseIntersections, int nextBaseIntersectionNumber,
			BigUInt partialCapacity, BigUInt totalCapacity) {
		for (int i = nextBaseIntersectionNumber; i < baseIntersections.size(); i++) {
			Space intersection = lastIntersection.intersect(baseIntersections.get(i));
			// stop recursion for this branch if there is no intersection
			if (intersection != null) {
				// add/sub the current capacity
				intersection.calculateCapacity(partialCapacity);
				try {
					if (minusSign) {
						totalCapacity.sub(partialCapacity);
					} else {
						totalCapacity.add(partialCapacity);
					}
				} catch (DifferentByteSizeException e) {
					// could not happen
				}
				// call recursion
				sieveFormulaRecursion(
					!minusSign, intersection, baseIntersections, i + 1,
					partialCapacity, totalCapacity
				);
			}
		}
	}
}
