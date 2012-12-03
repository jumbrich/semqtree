/*
 *
 */
package de.ilmenau.datasum.index;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import org.jdom.Element;

import de.ilmenau.datasum.config.Parameters;
import de.ilmenau.datasum.index.SelectConstraint.Operator;
import de.ilmenau.datasum.index.update.BucketIndexUpdateEntry;
import de.ilmenau.datasum.index.update.BucketsChangeInformation;
import de.ilmenau.datasum.index.update.IndexChangeInformation;
import de.ilmenau.datasum.index.update.IndexQualityInformation;
import de.ilmenau.datasum.index.update.IndexUpdateEntry;
import de.ilmenau.datasum.index.update.ItemIndexUpdateEntry;
import de.ilmenau.datasum.index.update.UpdateBucket;
import de.ilmenau.datasum.index.update.IndexUpdateEntry.UpdateAction;
import de.ilmenau.datasum.util.DataPoint;
import de.ilmenau.datasum.util.bigmath.BigMathHelper;
import de.ilmenau.datasum.util.bigmath.BigUInt;
import de.ilmenau.datasum.util.bigmath.DifferentByteSizeException;
import de.ilmenau.datasum.util.bigmath.Space;
import de.ilmenau.datasum.util.serialization.SerialBucketIndex;
import de.ilmenau.datasum.util.serialization.SerializationHelper;
/**
 * This class represents a Bucket based LocalIndex.
 * 
 * @author Christian Lemke
 * @version $Id$
 */
@SuppressWarnings("nls")
public abstract class BucketBasedLocalIndex extends LocalIndex {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	/** remember state of the local index before updates occured */
	private transient HashMap<String, BucketBasedLocalIndex> originalIndex = null;
	/** remember all those data updates to this index that have not been propagated to any neighbors */
	private transient HashMap<String, Vector<IndexUpdateEntry>> recentUpdates = null;
	/** cache of the latest index change information */
	private transient HashMap<String, IndexChangeInformation> changeInformationCache = null;

	/** cache of the size for an {@link BucketIndexUpdateEntry} */
	private transient int sizeForBucketIndexUpdateEntry = 0;
	/** cache of the size for an {@link ItemIndexUpdateEntry} */
	private transient int sizeForItemIndexUpdateEntry = 0;
	
	
	/** enum of possible update strategies */
	public enum UpdateStrategy {
		/** do not propagate updates (disable propagation) and do not update the local index */
		NO_PROPAGATION,
		/** updates are sent immediately without checking conditions */
		IMMEDIATELY,
		/**
		 * same as IMMEDIATELY, but while message processing on a peer all UpdateEventMessages
		 * are evaluated.
		 * only used for evaluation as reference strategy to compare to.
		 */
		EVALUATION_REFERENCE,
		/**
		 * compute the change information for all neighbors on a peer together and
		 * send all recent updates to all neighbors simultaneously.
		 * <ul>
		 * 	<li>only based on optional thresholds</li>
		 * 	<li>only one copy of an index is needed for comparison!</li>
		 * </ul>
		 */
		SIMPLE_THRESHOLD,
		/**
		 * compute the change information for each neighbor on a peer separately and
		 * decide for each neighbor whether to send updates to it or not.
		 * <ul>
		 * 	<li>only based on optional thresholds</li>
		 * 	<li>for each neighbor a copy of an index is needed for comparison!</li>
		 * </ul>
		 */
		ADVANCED_THRESHOLD,
		/**
		 * every peer ask his neighbors periodically for updates.
		 * so the update propagation is not initiated by new updates on a peer!
		 */
		PROACTIVE,
		/**
		 * a <tt>SelectPop</tt> contains information about the amount of expected results.
		 * if the current results differ from the expected results then update are sent with
		 * the answer.
		 * if a peer did not send updates to a neighbor a long time then a UpdateEventMessage
		 * will be send.
		 * so the update propagation is not initiated by new updates on a peer!
		 */
		QUERY_ESTIMATION,
		/**
		 * the results of a <tt>SelectPop</tt> are used to update the index.
		 * so the update propagation is not initiated by new updates on a peer!
		 */
		QUERY_FEEDBACK
	}


	/**
	 * constructor.
	 * 
	 * @param type the index type
	 */
	public BucketBasedLocalIndex(IndexType type) {
		super(type);
	}

	/**
	 * method that aggregate updates if necessary.
	 * (create and set updates as BucketAURList -- ADD/UPDATE/REMOVE buckets.)
	 * The recent updates (data item and/or buckets) will be replaced by the changed buckets.
	 *  
	 * @param neighborID for which neighbor to aggregate the updates 
	 */
	public void aggregateUpdates(String neighborID) {
		// TODO <ChL>: consider hops!?
    	// if there are no updates do nothing
		if (this.recentUpdates == null || this.recentUpdates.isEmpty()) {
			return;
		}
		Vector<IndexUpdateEntry> recentUpdatesForNeighbor = this.recentUpdates.get(neighborID);
		if (recentUpdatesForNeighbor == null || recentUpdatesForNeighbor.isEmpty()) {
			return;
		}

		// calculate the size for sending the changed buckets
		IndexChangeInformation ici = this.computeChangeInformation(neighborID);
		int sizeForChangedBucketsUpdates
			= ici.getNmbOfChangedBuckets() * this.getSizeForBucketIndexUpdateEntry();
		// calculate the size for sending the current updates
		int sizeForCurrentUpdates = 0;
		for (IndexUpdateEntry iue : recentUpdatesForNeighbor) {
			if (iue instanceof ItemIndexUpdateEntry) {
				sizeForCurrentUpdates += this.getSizeForItemIndexUpdateEntry();
			} else if (iue instanceof BucketIndexUpdateEntry) {
				sizeForCurrentUpdates += this.getSizeForBucketIndexUpdateEntry();
			}
		}
		// only aggregate if reasonable
		if (sizeForChangedBucketsUpdates >= sizeForCurrentUpdates) {
			return;
		}
		
		// get all (old and new) buckets
		HashMap<String, Bucket> newBuckets = getAllBucketsAsHashMapGlobalID();
		HashMap<String, Bucket> oldBuckets = this.originalIndex.get(neighborID).getAllBucketsAsHashMapGlobalID();
		
		Vector<IndexUpdateEntry> newUpdates = new Vector<IndexUpdateEntry>();
		
		// process new buckets
		for (Map.Entry<String, Bucket> currEntry : newBuckets.entrySet()) {
			// find corresponding bucket in the oldBuckets Map
			Bucket newBucket = currEntry.getValue();
			Bucket corrOldBucket = oldBuckets.get(currEntry.getKey());

			if (corrOldBucket == null) {
				// if there is a new bucket in the actual index
				newUpdates.add(new BucketIndexUpdateEntry(
					IndexUpdateEntry.UpdateAction.ADD_BUCK, 0,
					newBucket.createUpdateBucket()
				));
			} else {
				// only create updates for changed buckets!
				if (hasBucketChanged(newBucket, corrOldBucket)) {
					// create the update-bucket
					int integerPeerID = -1;
					if (newBucket.getPeerID() != null) {
						try {
							integerPeerID = Integer.parseInt(newBucket.getPeerID());
						} catch (NumberFormatException nfe) {
							// do nothing
						}
					}
					int integerNeighborID = -1;
					if (newBucket.getNeighborID() != null) {
						try {
							integerNeighborID = Integer.parseInt(newBucket.getNeighborID());
						} catch (NumberFormatException nfe) {
							// do nothing
						}
					} 
					UpdateBucket updateBucket = new UpdateBucket(
						newBucket.getLowerBoundaries(), newBucket.getUpperBoundaries(),
						(newBucket.getCount() - corrOldBucket.getCount()),
						newBucket.getAttributeNames(), integerPeerID, integerNeighborID,
						newBucket.getLocalBucketID()
					);
					// only for evaluation forward the points to add/remove in the bucket!
					if (Parameters.storeDataPointsInBuckets) {
						Vector<DataPoint> pointsToAdd = new Vector<DataPoint>(newBucket.getContainedPoints());
						Vector<DataPoint> pointsToRemove = new Vector<DataPoint>(corrOldBucket.getContainedPoints());
						
						Iterator<DataPoint> itAdd = pointsToAdd.iterator();
						while (itAdd.hasNext()) {
							Iterator<DataPoint> itRemove = pointsToRemove.iterator();
							while (itRemove.hasNext()) {
								// if a point is in both buckets, ignore it
								if (itAdd.next().equals(itRemove.next())) {
									itAdd.remove();
									itRemove.remove();
									break;
								}
							}
						}
						
						updateBucket.addPoints(pointsToAdd);
						updateBucket.setPointsToRemove(pointsToRemove);
					}
					// if there is a new bucket in the actual index
					newUpdates.add(new BucketIndexUpdateEntry(
						IndexUpdateEntry.UpdateAction.UPD_BUCK, 0, updateBucket
					));
				}
				// remove correlated old bucket from HashMap
				oldBuckets.remove(currEntry.getKey());
			}
		}
		// process removed buckets
		for (Map.Entry<String, Bucket> currEntry : oldBuckets.entrySet()) {
			newUpdates.add(new BucketIndexUpdateEntry(
				IndexUpdateEntry.UpdateAction.RMV_BUCK, 0,
				currEntry.getValue().createUpdateBucket()
			));
		}
		
		this.recentUpdates.put(neighborID, newUpdates);
		if (this.changeInformationCache != null) {
			this.changeInformationCache.remove(neighborID);
		}
	}

	/**
	 * method that creates a copy of the local index if necessary for later comparison.
	 * (change information)
	 * 
	 * @param neighborIDsToConsider for which neighbors a copy should be cretaed 
	 */
	public void cloneIndexIfNecessary(Vector<String> neighborIDsToConsider) {
		if (this.originalIndex == null) {
			this.originalIndex = new HashMap<String, BucketBasedLocalIndex>();
		}
		for (String neighbor : neighborIDsToConsider) {
			if (this.originalIndex.get(neighbor) == null) {
				this.originalIndex.put(neighbor, (BucketBasedLocalIndex) this.clone());
			}
		}
	}

	/**
	 * method that computes the index change information for all neighbors which contain updates
	 * to this index that have not been propagated.
	 * 
	 * @return the IndexChangeInformation for the neighbors ("@" if for all neighbors together)
	 */
	public HashMap<String, IndexChangeInformation> computeAllChangeInformations() {
		HashMap<String, IndexChangeInformation> result =  new HashMap<String, IndexChangeInformation>();
		if (this.originalIndex != null) {
			for (String neighborID : this.originalIndex.keySet()) {
				result.put(neighborID, computeChangeInformation(neighborID));
			}
		}
		return result;
	}

	/**
	 * Compare the original index and the current index and calculate the change information.
	 * 
	 * The comparison only considers the buckets since they are forwarded to other peers and the
	 * local change information is later on compared to the change information of neighboring peers.
	 * 
	 * @param neighborID for which neighbor to calculate the change information 
	 * @return the change information
	 */
    public IndexChangeInformation computeChangeInformation(String neighborID) {
    	// if there are no updates the local change information is empty
		if (this.recentUpdates == null || this.recentUpdates.isEmpty()) {
			return new IndexChangeInformation();
		}
		Vector<IndexUpdateEntry> recentUpdatesForNeighbor = this.recentUpdates.get(neighborID);
		if (recentUpdatesForNeighbor == null || recentUpdatesForNeighbor.isEmpty()) {
			return new IndexChangeInformation();
		}
		// informations already in the cache?
		if (this.changeInformationCache != null) {
			IndexChangeInformation changeInformationCacheForNeighbor = this.changeInformationCache.get(neighborID);
			if  (changeInformationCacheForNeighbor != null) {
				return changeInformationCacheForNeighbor;
			}
		} else {
			this.changeInformationCache = new HashMap<String, IndexChangeInformation>();
		}

		// get all (old and new) buckets
		HashMap<String, Bucket> newBuckets = getAllBucketsAsHashMapGlobalID();
		HashMap<String, Bucket> oldBuckets = this.originalIndex.get(neighborID).getAllBucketsAsHashMapGlobalID();

		BucketsChangeInformation bci = new BucketsChangeInformation();
		// process new buckets
		for (Map.Entry<String, Bucket> currEntry : newBuckets.entrySet()) {
			// find corresponding bucket in the oldBuckets Map
			Bucket corrOldBucket = oldBuckets.get(currEntry.getKey());

			if (corrOldBucket == null) {
				// if there is a new bucket in the actual index
				bci.add(computeBucketChangeInformation(currEntry.getValue(), true));
			} else {
				bci.add(computeBucketChangeInformation(currEntry.getValue(), corrOldBucket));
				// remove correlated old bucket from HashMap
				oldBuckets.remove(currEntry.getKey());
			}
		}
		// process removed buckets
		for (Map.Entry<String, Bucket> currEntry : oldBuckets.entrySet()) {
			bci.add(computeBucketChangeInformation(currEntry.getValue(), false));
		}

		// create index change information
		IndexChangeInformation ici = new IndexChangeInformation(bci, recentUpdatesForNeighbor.size());
		this.changeInformationCache.put(neighborID, ici);
		return ici;
	}
    

	/**
	 * Compare the original index and the current index and calculate the change information.
	 * 
	 * The comparison only considers the buckets since they are forwarded to other peers and the
	 * local change information is later on compared to the change information of neighboring peers.
	 * 
	 * @param neighborIDsToConsider for which neighbors to calculate the change information 
	 * @return vector with the change information
	 */
    public Vector<IndexChangeInformation> computeChangeInformations(Vector<String> neighborIDsToConsider) {
    	Vector<IndexChangeInformation> result = new Vector<IndexChangeInformation>();
    	for (String neighborID : neighborIDsToConsider) {
    		result.add(computeChangeInformation(neighborID));
    	}
    	return result;
    }

    /**
     * method that computes the quality information of this index.
     * 
     * @return the index quality information
     */
    public IndexQualityInformation computeQualityInformation() {
    	Vector<Bucket> buckets = this.getAllBuckets(false);
		if (buckets.isEmpty()) {
			// return empty infos if the are no buckets
			return new IndexQualityInformation();
		}

		if (!Parameters.storeDataPointsInBuckets) {
			throw new IllegalStateException(
				"to gather index quality information the setting " +
				"NetworkConfig.storeDataPointsInBuckets has to be enabled!"
			);
		}

		int nmbOfDimensions = this.getNmbOfDimensions();
		// create temporary variable for the average bucket length
		double totalAvgBucketlength = 0.0;
		// create temporary variable for the bucket density
		double totalBucketsDensities = 0.0;
		// create temporary variables for the maximum error of a data point
		// --> first: determine the number of indexed items
		int nmbOfPointsInIndex = 0;
		for (Bucket bucket : buckets) {
			nmbOfPointsInIndex += bucket.getContainedPoints().size();
		}
		// second: estimate needed bytes for the BigUInt
		double avgMaximumDistances;
		int estimatedByteSizeNeededForDistance = BigMathHelper.getByteSizeToUse(
			BigMathHelper.getEstimatedCornerDistanceBitSize(
				nmbOfDimensions, nmbOfPointsInIndex
			)
		);
		BigUInt totalMaximumDistances = new BigUInt(estimatedByteSizeNeededForDistance);
		BigUInt maximumDistance = new BigUInt(estimatedByteSizeNeededForDistance);


		int[] lowerBoundaries;
		int[] upperBoundaries;
		for (Bucket bucket : buckets) {
			// calculate the current average bucket length
			// --> iterate over all dimensions
			lowerBoundaries = bucket.getLowerBoundaries();
			upperBoundaries = bucket.getUpperBoundaries();
			double avgBucketlength = 0.0;
			for (int i = 0; i < nmbOfDimensions; i++) {
				avgBucketlength += (upperBoundaries[i] - lowerBoundaries[i] + 1);
			}
			totalAvgBucketlength += (avgBucketlength / nmbOfDimensions);

			// calculate the bucket density
			// --> calculate the space for the capacity (Bytes for BigUInt)
			int estimatedByteSizeNeededForCapacity = BigMathHelper.getByteSizeToUse(
				BigMathHelper.getEstimatedCapacityBitSize(bucket)
			);
			// --> then calculate the number of unique data points in the bucket & bucket capacity
			BigUInt nmbOfUniquePoints = new BigUInt(estimatedByteSizeNeededForCapacity);
			bucket.getNmbOfUniquePoints(nmbOfUniquePoints);
			BigUInt bucketCapacity = new BigUInt(estimatedByteSizeNeededForCapacity);
			bucket.calculateCapacity(bucketCapacity);
			// --> at last calculate the ratio (number of unique data points in the bucket / bucket capacity)
			try {
				// 'FIXPOINT_RANGE_EXTENSION * 8' bits dual floating points
				nmbOfUniquePoints.shiftLeft(BigMathHelper.FIXPOINT_RANGE_EXTENSION * 8);
				nmbOfUniquePoints.div(bucketCapacity);
			} catch (DifferentByteSizeException e) {
				// could not happen
			}
			// add info to total info
			totalBucketsDensities += nmbOfUniquePoints.getLower63Bits() 
										/ Math.pow(2.0, BigMathHelper.FIXPOINT_RANGE_EXTENSION * 8);

			// calculate the maximum distances
			// --> iterate over all points in the bucket
			try {
				Vector<DataPoint> points = bucket.getContainedPoints();
				for (DataPoint point : points) {
					bucket.calculateMaxDistance(point, maximumDistance);
					totalMaximumDistances.add(maximumDistance);
				}
			} catch (DifferentByteSizeException e) {
				// cannot happen
			}
		}
		// DEBUG: to remove. only for testing the implementation
		// --> in query feedback the count may vary due estimation errors
		if ((!Parameters.updateStrategy.equals(UpdateStrategy.QUERY_FEEDBACK)) 
				&& (nmbOfPointsInIndex != this.getNmbOfIndexedItems())) {
			throw new IllegalStateException(
				"the index information 'nmbOfIndexedItems' (" + this.getNmbOfIndexedItems()
				+ ") do not conform to the number of contained points (" + nmbOfPointsInIndex + ")!"
			);
		}

		// calculate average of maximum errors (over all data points)
		maximumDistance.setZero();
		maximumDistance.add(nmbOfPointsInIndex);
		try {
			totalMaximumDistances.div(maximumDistance);
		} catch (DifferentByteSizeException e) {
			// cannot happen
		}
		avgMaximumDistances= totalMaximumDistances.getLower63Bits()
								/ Math.pow(2.0, BigMathHelper.FIXPOINT_RANGE_EXTENSION * 4);

		return new IndexQualityInformation(
			totalAvgBucketlength / buckets.size(), totalBucketsDensities / buckets.size(),
			avgMaximumDistances
		);
	}

	/**
	 * create updates (ItemARList -- ADD/REMOVE items) from changes.
	 * 
	 * @param removeList Elements that should be removed 
	 * @param addList Elements that should be added
	 * @return Vector of updates (ItemIndexUpdateEntry)
	 */
	public Vector<IndexUpdateEntry> createItemARList(Vector<Element> removeList, Vector<Element> addList) {
		// copy list of attribute names (per dimension)
		Vector<String> attributeNames = this.getAttributeNames();
		int cntDims = attributeNames.size();
		String[] dimensions = attributeNames.toArray(new String[cntDims]);
		Vector<IndexUpdateEntry> ARlist = new Vector<IndexUpdateEntry>();
		
		/*System.out.println(attributeNames);
		System.out.println(removeList);
		for (Element currElem: removeList){
			System.out.println("\t "+currElem.getChildren());
		}*/

		// create list of elements to remove
		for (Element nextUpdate : removeList) {
			//System.out.println("remove: " + nextUpdate.getChildren());
			int[] coordinates = new int[cntDims];
			for (int i = 0; i < cntDims; i++) {
				coordinates[i] = Integer.parseInt(nextUpdate.getChildText(dimensions[i]));
			}

			IndexUpdateEntry newUpdateEntry = new ItemIndexUpdateEntry(
				IndexUpdateEntry.UpdateAction.RMV_ITEM, dimensions, coordinates, 0
			);
			ARlist.add(newUpdateEntry);
		}

		// create list of elements to add
		for (Element nextUpdate : addList) {
			//System.out.println("add: " + nextUpdate.getChildren());
			int[] coordinates = new int[cntDims];
			for (int i = 0; i < cntDims; i++) {
				coordinates[i] = Integer.parseInt(nextUpdate.getChildText(dimensions[i]));
			}

			IndexUpdateEntry newUpdateEntry = new ItemIndexUpdateEntry(
				IndexUpdateEntry.UpdateAction.ADD_ITEM, dimensions, coordinates, 0
			);
			ARlist.add(newUpdateEntry);
		}
		return ARlist;
	}

	/**
	 * abstract method that returns the buckets of the index.
	 * 
	 * @param clone if the buckets should be cloned
	 * @return Vector of Buckets
	 */
	public abstract Vector<Bucket> getAllBuckets(boolean clone);

	/**
	 * abstract method that returns all buckets as HashMap with the global bucketID as key.
	 * 
	 * @return list of all buckets in the index
	 */
	public abstract HashMap<String, Bucket> getAllBucketsAsHashMapGlobalID();

	/**
	 * abstract method that returns the buckets (and intersection information) of the index in the
	 * given query space.
	 * 
	 * @param qs the query space
	 * @return Vector of IntersectionInformation
	 */
	public abstract Vector<IntersectionInformation> getAllBucketsInQuerySpace(QuerySpace qs);

	/**
     * method that returns all recent updates.
     * 
     * @return HashMap (String -> neighborID; Vector of IndexUpdateEntry)
     */
    public HashMap<String, Vector<IndexUpdateEntry>> getAllRecentUpdates() {
    	return this.recentUpdates;
    }

	/**
	 * abstract method that returns the names of the attributes (for each dimension).
	 * 
	 * @return the names of the attributes
	 */
	public abstract Vector<String> getAttributeNames();

	/**
	 * abstract method that returns the number of buckets used for the indexed items (elements).
	 * 
	 * @return the number of buckets used for the indexed items (elements)
	 */
	public abstract int getNmbOfBuckets();

    /**
	 * abstract method that returns the number of dimensions of the index.
	 * 
	 * @return the number of dimensions of the index
	 */
	public abstract int getNmbOfDimensions();

    /**
	 * abstract method that returns the number of indexed items in the index.
	 * 
	 * @return the number of indexed items in the index
	 */
	public abstract int getNmbOfIndexedItems();

	/**
	 * method that generates a QuerySpace from the given XPath expression.
	 * @see #getSelectConstraintsFromXPath(String)
	 * 
	 * @param expression the XPath expression
	 * @return the corresponding query space
	 */
	public QuerySpace getQuerySpaceFromXPath(String expression) {
		Vector<String> attributeNames = this.getAttributeNames();
		int nmbOfDimensions = attributeNames.size();
		int[] lowerBoundaries = new int[nmbOfDimensions];
		int[] upperBoundaries = new int[nmbOfDimensions];
		
		for (int i = 0; i < nmbOfDimensions; i++) {
			// min + 1 because length=upperBoundary-lowerBoundary+1 so that no overflow occurs
			lowerBoundaries[i] = Integer.MIN_VALUE + 1; // = -(Integer.MAX_VALUE)
			upperBoundaries[i] = Integer.MAX_VALUE;
		}
		// get constraints (XPath predicates)
		Vector<SelectConstraint> constraints = getSelectConstraintsFromXPath(expression);
		// now restrict the query space
		boolean querySpaceNotRestricted = true;
		for (SelectConstraint constraint : constraints) {
			int index = constraint.getElementIndex();
			int value = constraint.getElementValue();
			if (lowerBoundaries[index] == upperBoundaries[index]) {
				// ignore constraint because no more restriction is possible
				continue;
			}
			switch (constraint.getComparisonOperator()) {
				case SMALLER:
					if (value > lowerBoundaries[index]) {
						upperBoundaries[index] = Math.min(value - 1, upperBoundaries[index]);
						querySpaceNotRestricted = false;
					} else {
						System.err.println(
							"BucketBasedLocalIndex.getQuerySpaceFromXPath: " +
							"invalid constraints combination (SMALLER)"
						);
					}
					break;
				case SMALLER_OR_EQUAL:
					if (value >= lowerBoundaries[index]) {
						upperBoundaries[index] = Math.min(value, upperBoundaries[index]);
						querySpaceNotRestricted = false;
					} else {
						System.err.println(
							"BucketBasedLocalIndex.getQuerySpaceFromXPath: " +
							"invalid constraints combination (SMALLER_OR_EQUAL)"
						);
					}
					break;
				case EQUAL:
					// TODO: detect invalid constraints combinations: x>10 and x=10 (example)
					// --> now the = is stronger than the other operators
					lowerBoundaries[index] = value;
					upperBoundaries[index] = value;
					querySpaceNotRestricted = false;
					break;
				case GREATER_OR_EQUAL:
					if (value <= upperBoundaries[index]) {
						lowerBoundaries[index] = Math.max(value, lowerBoundaries[index]);
						querySpaceNotRestricted = false;
					} else {
						System.err.println(
							"BucketBasedLocalIndex.getQuerySpaceFromXPath: " +
							"invalid constraints combination (GREATER_OR_EQUAL)"
						);
					}
					break;
				case GREATER:
					if (value < upperBoundaries[index]) {
						lowerBoundaries[index] = Math.max(value + 1, lowerBoundaries[index]);
						querySpaceNotRestricted = false;
					} else {
						System.err.println(
							"BucketBasedLocalIndex.getQuerySpaceFromXPath: " +
							"invalid constraints combination (GREATER)"
						);
					}
					break;
			}
		}
		return new QuerySpace(lowerBoundaries, upperBoundaries, querySpaceNotRestricted);
	}

	/**
     * method that returns the recent updates.
     * 
	 * @param neighborID for which neighbor to get the recent updates
     * @return Vector of IndexUpdateEntry
     */
    public Vector<IndexUpdateEntry> getRecentUpdates(String neighborID) {
    	if (this.recentUpdates == null) {
    		return null;
    	}
    	return this.recentUpdates.get(neighborID);
    }

	/**
	 * method that generates SelectConstraint from the given XPath expression.
	 * <br>
	 * examples of supported XPath expressions:
	 * <ul>
	 * 	<li>"//item"</li> -> no constraints
	 * 	<li>"//item[a>1]"</li> -> <tt>a</tt> must be greater than one
	 * 	<li>"//item[a=1 and b<2 and c>3 and c<=4 and d>=5]"</li> -> multiple constraints
	 * </ul>
	 * currently attributes are not supported and only the first predicates ([]) connected with
	 * <tt>and</tt> are evaluated!<br>
	 * <br>
	 * if an element is not indexed then it is ignored as constraint.
	 * --> TODO: give feedback to the caller of this function (special SelectConstraint?) 
	 * 
	 * @param expression the XPath expression
	 * @return the select constraints
	 */
	public Vector<SelectConstraint> getSelectConstraintsFromXPath(String expression) {
		Vector<SelectConstraint> result = new Vector<SelectConstraint>();
		int predicatesStartPos = expression.indexOf("[");
		// return empty SelectConstraint vector if there are no predicates
		if (predicatesStartPos == -1) {
			return result;
		}

		int predicatesEndPos = expression.indexOf("]");
		// get string with predicates
		String predicatesString = expression.substring(predicatesStartPos + 1, predicatesEndPos);
		// remove ' and " characters
		predicatesString = predicatesString.replace("'", "");
		predicatesString = predicatesString.replace("\"", "");
		// return empty SelectConstraint vector if there are no predicates
		if (predicatesString.length() == 0) {
			return result;
		}

		// get vector of indexed element names
		Vector<String> attributeNames = this.getAttributeNames();
		// define some variables used in the following loop
		int lessPosition;
		int equalPosition;
		int greaterPosition;
		Operator op;
		int elementIndex;
		String splitParam;
		String[] keyValuePair;
		// extract predicates
		String[] predicates = predicatesString.split(" and ");
		for (String predicate : predicates) {
			// remove blanks
			predicate = predicate.replace(" ", "");
			// determine positions of the operator signs
			lessPosition = predicate.indexOf("<");
			equalPosition = predicate.indexOf("=");
			greaterPosition = predicate.indexOf(">");
			if (lessPosition != -1) {
				if (equalPosition != -1) {
					op = Operator.SMALLER_OR_EQUAL;
					splitParam = (lessPosition < equalPosition) ? "<=" : "=<";
				} else {
					op = Operator.SMALLER;
					splitParam = "<";
				}
			} else {
				if (greaterPosition != -1) {
					if (equalPosition != -1) {
						op = Operator.GREATER_OR_EQUAL;
						splitParam = (greaterPosition < equalPosition) ? ">=" : "=>";
					} else {
						op = Operator.GREATER;
						splitParam = ">";
					}
				} else {
					if (equalPosition == -1) {
						// invalid/unknown operator
						System.err.println(
							"BucketBasedLocalIndex.getSelectConstraintsFromXPath: " +
							"no or invalid operator in predicate"
						);
						continue;
					}
					// else it's only an equal sign
					op = Operator.EQUAL;
					splitParam = "=";
				}
			}
			// get key (name of element) and value (value of element) pair 
			keyValuePair = predicate.split(splitParam);
			// get index of the element name
			elementIndex = attributeNames.indexOf(keyValuePair[0]);
			// if the element is indexed then add the constraint
			if (elementIndex > -1) {
				result.add(
					new SelectConstraint(elementIndex, Integer.parseInt(keyValuePair[1]), op)
				);
			}
		}

		// return result vector
		return result;
	}

	/**
	 * method that returns a serializable BucketBasedLocalIndex for java.beans.XMLEncoder.
	 * Overwrite this method in sub classes for other SerialBucketIndexes.
	 * 
	 * @return SerialBucketIndex
	 */
	public SerialBucketIndex getSerializableBucketIndex() {
		/*
		Vector<SerialBucket> serialBucketList = new Vector<SerialBucket>();
		Vector<Bucket> bucketList = getAllBuckets(false);
		if (!bucketList.isEmpty()) {
			for (Bucket b :  bucketList) {
				serialBucketList.add(b.getSerializableBucket());
			}
		}
		*/

		int nmbOfIndexCopies = 0;
		int nmbOfBucketsInIndexCopies = 0;
		if (this.originalIndex != null) {
			for (BucketBasedLocalIndex LI : this.originalIndex.values()) {
				nmbOfIndexCopies++;
				nmbOfBucketsInIndexCopies += LI.getNmbOfBuckets();
			}
		}

		return new SerialBucketIndex(
			this.getNmbOfIndexedItems(), this.getNmbOfBuckets(),
			nmbOfIndexCopies, nmbOfBucketsInIndexCopies
		);
	}

	/**
	 * @return returns the maximum size for a BucketIndexUpdateEntry
	 */
	public int getSizeForBucketIndexUpdateEntry() {
		// determine size if not already done
		if (this.sizeForBucketIndexUpdateEntry == 0) {
			Vector<String> attributeNames = getAttributeNames();
			int cntDims = attributeNames.size();
			int[] pos1 = new int[cntDims];
			int[] pos2 = new int[cntDims];

			UpdateBucket updateBucket = new UpdateBucket(
				pos1, pos2, 1, attributeNames, 2, 3, 0
			);
			BucketIndexUpdateEntry biue = new BucketIndexUpdateEntry(
				UpdateAction.RMV_BUCK, 0, updateBucket
			);
			try {
				this.sizeForBucketIndexUpdateEntry = SerializationHelper.getSerializedSizeOfObject(biue);
			} catch (IOException e) {
				// should not happen!
				e.printStackTrace();
			}
		}
		return this.sizeForBucketIndexUpdateEntry;
	}

	/**
	 * @return returns the maximum size for an ItemIndexUpdateEntry
	 */
	public int getSizeForItemIndexUpdateEntry() {
		// determine size if not already done
		if (this.sizeForItemIndexUpdateEntry == 0) {
			Vector<String> attributeNames = getAttributeNames();
			int cntDims = attributeNames.size();
			String[] dimensions = attributeNames.toArray(new String[cntDims]);
			int[] pos = new int[cntDims];

			ItemIndexUpdateEntry iiue = new ItemIndexUpdateEntry(
				UpdateAction.RMV_ITEM, dimensions, pos, 0
			);
			try {
				this.sizeForItemIndexUpdateEntry = SerializationHelper.getSerializedSizeOfObject(iiue);
			} catch (IOException e) {
				// should not happen!
				e.printStackTrace();
			}
		}
		return this.sizeForItemIndexUpdateEntry;
	}

	/**
	 * abstract method that returns whether the boundaries of a bucket can be changed.
	 * 
	 * @return <tt>true</tt> if the boundaries of a bucket can not be changed
	 */
	public abstract boolean hasFixedBucketBoundaries();

	/**
	 * abstract method that inserts a new bucket (update) into the index.
	 * If the bucket already exists an exception should be thrown!
	 * 
	 * @param bucket the bucket to add
	 * @throws Exception if the bucket exists or contains invalid data
	 */
	public abstract void insertBucket(UpdateBucket bucket, boolean insertIntoExistingBucketIfPossible) throws Exception;

	/**
	 * abstract method that inserts a single multidimensional data point.
	 * This method is used to create an update an index!
	 * 
	 * @param coordinates the coordinates of the point
	 * @throws Exception
	 */
	public abstract void insertDataItem(int[] coordinates, String sourceID) throws Exception;

	/**
	 * method that removes the given updates from the recent updates.
	 * 
	 * @param theARList the list with updates to remove
	 * @param neighborID for which neighbor to remove the given updates
	 */
	public void removeARListEntriesFromRecentUpdates(Vector<IndexUpdateEntry> theARList, String neighborID) {
		if (this.recentUpdates != null) {
			Vector<IndexUpdateEntry> recentUpdatesForNeighbor = this.recentUpdates.get(neighborID);
			if (recentUpdatesForNeighbor != null && !recentUpdatesForNeighbor.isEmpty()) {
				recentUpdatesForNeighbor.removeAll(theARList);
				// clear the cache!
				if (this.changeInformationCache != null) {
					this.changeInformationCache.remove(neighborID);
				}
			}
		}
	}

	/**
	 * abstract method that removes an existing bucket (update) + infos from the index.
	 * If the bucket not exists an exception should be thrown!
	 * 
	 * @param bucket the bucket to remove
	 * @throws Exception if the bucket not exists or contains invalid data
	 */
	public abstract void removeBucket(UpdateBucket bucket) throws Exception;

    /**
	 * abstract method that removes a single multidimensional data point.
	 * 
	 * @param coordinates the coordinates of the point
	 * @throws Exception
	 */
	public abstract void removeDataItem(int[] coordinates) throws Exception;

    /**
	 * method that resets all recents updates and the copy of the index.
	 * 
     * @param neighborID for which neighbor to reset the recent updates 
	 */
	public void resetRecentUpdates(String neighborID) {
		if (this.recentUpdates != null) {
			this.recentUpdates.remove(neighborID);
		}
		if (this.originalIndex != null) {
			this.originalIndex.remove(neighborID);
		}
		if (this.changeInformationCache != null) {
			this.changeInformationCache.remove(neighborID);
		}
	}

	/**
	 * method that adapts the index by inserting data items and remembers update entries. 
	 * 
	 * @param theARList the list with updates (IndexUpdateEntry) 
	 * @param neighborIDsToConsider for which neighbors to save the recent updates 
	 * @throws Exception if an error occurs while updating 
	 */
	public void update(Vector<IndexUpdateEntry> theARList, Vector<String> neighborIDsToConsider) throws Exception {
		// remember updates (if necessary)
		if (neighborIDsToConsider != null) {
			if (this.recentUpdates == null) {
				this.recentUpdates = new HashMap<String, Vector<IndexUpdateEntry>>();
			}
			for (String neighborID : neighborIDsToConsider) {
				Vector<IndexUpdateEntry> recentUpdatesForNeighbor = this.recentUpdates.get(neighborID);
				if (recentUpdatesForNeighbor == null) {
					recentUpdatesForNeighbor = new Vector<IndexUpdateEntry>();
				}
				recentUpdatesForNeighbor.addAll(theARList);
				this.recentUpdates.put(neighborID, recentUpdatesForNeighbor);
			}
			// reset cache
			if (this.changeInformationCache != null) {
				for (String neighborID : neighborIDsToConsider) {
					this.changeInformationCache.remove(neighborID);
				}
			}
		}
		// optimization: first process the remove update-entries
		// --> give the index a chance to remove big buckets and to create new small buckets
		//     (usually buckets only become bigger because there are no exact information about the items!)
		for (IndexUpdateEntry currUpdate : theARList) {
			if (currUpdate.getAction() == IndexUpdateEntry.UpdateAction.RMV_ITEM) {
				removeDataItem(((ItemIndexUpdateEntry) currUpdate).getCoordinates());
			} else if (currUpdate.getAction() == IndexUpdateEntry.UpdateAction.RMV_BUCK) {
				removeBucket(((BucketIndexUpdateEntry) currUpdate).getBucket());
			}
		}
		for (IndexUpdateEntry currUpdate : theARList) {
			if (currUpdate.getAction() == IndexUpdateEntry.UpdateAction.UPD_BUCK) {
				updateBucket(((BucketIndexUpdateEntry) currUpdate).getBucket());
			} else if (currUpdate.getAction() == IndexUpdateEntry.UpdateAction.ADD_ITEM) {
				insertDataItem(((ItemIndexUpdateEntry) currUpdate).getCoordinates(), null);
			} else if (currUpdate.getAction() == IndexUpdateEntry.UpdateAction.ADD_BUCK) {
				insertBucket(((BucketIndexUpdateEntry) currUpdate).getBucket(),true);
			}
		}
	}

	/**
	 * abstract method that updates an existing bucket (update) + infos.
	 * The bucket should contain only the differences. (example: bucket.count=new_count - old_count)
	 * If the bucket not exists an exception should be thrown!
	 * 
	 * @param bucket the bucket with infos to change
	 * @throws Exception if the bucket not exists or contains invalid data
	 */
	public abstract void updateBucket(UpdateBucket bucket) throws Exception;

	/**
	 * method that computes the intersection information (between the given bucket and the query
	 * space (for example: the ratio between the intersection and the total size of the bucket)<br>
	 * <br>
	 * assumption: there is an intersection !
	 * 
	 * @param bucket the bucket
	 * @param qs the query space
	 * @return the intersection information
	 */
	public IntersectionInformation computeIntersectionInformation(Bucket bucket, QuerySpace qs) {
		// compute the intersection space
		Space intersection = bucket.intersect(qs);
		// --> calculate the space needed for the bucket capacity (BigUInt)
		int estimatedByteSizeNeeded = BigMathHelper.getByteSizeToUse(
			BigMathHelper.getEstimatedCapacityBitSize(bucket)
		);
		BigUInt bucketCapacity = new BigUInt(estimatedByteSizeNeeded);
		BigUInt intersectionCapacity = new BigUInt(estimatedByteSizeNeeded);
		// --> calculate the capacities
		bucket.calculateCapacity(bucketCapacity);
		intersection.calculateCapacity(intersectionCapacity);
		// --> at last calculate the ratio (intersection capacity / bucket capacity)
		try {
			// 'FIXPOINT_RANGE_EXTENSION * 8' bits dual floating points
			intersectionCapacity.shiftLeft(BigMathHelper.FIXPOINT_RANGE_EXTENSION * 8);
			intersectionCapacity.div(bucketCapacity);
		} catch (DifferentByteSizeException e) {
			// could not happen
		}

		return new IntersectionInformation(
			bucket, intersection,
			(intersectionCapacity.getLower63Bits()
				/ Math.pow(2.0, BigMathHelper.FIXPOINT_RANGE_EXTENSION * 8))
		);
	}

	/**
	 * method that computes the change information for <b>one</b> bucket (added/removed).
	 * 
	 * @param bucket the bucket
     * @param isAdded <tt>true</tt> if the bucket was added to the index, <tt>false</tt> if removed
	 * @return the change information
	 */
	private BucketsChangeInformation computeBucketChangeInformation(Bucket bucket, boolean isAdded) {
		// iterate over all dimensions
		int[] lowerBoundaries = bucket.getLowerBoundaries();
		int[] upperBoundaries = bucket.getUpperBoundaries();
		int nmbOfDimensions = lowerBoundaries.length;
		double avgBucketLength = 0.0; // the average length of the new bucket
		double maxBucketLength = 0.0; // the maximum length of the new bucket
		for (int i = 0; i < nmbOfDimensions; i++) {
			double currentLength = upperBoundaries[i] - lowerBoundaries[i] + 1;
			avgBucketLength += currentLength;
			if (currentLength > maxBucketLength) {
				maxBucketLength = currentLength;
			}
		}
		avgBucketLength = avgBucketLength / nmbOfDimensions;

		return new BucketsChangeInformation(
			// information _together_ for the old and new index
			1, 1, bucket.getCount(), avgBucketLength, maxBucketLength,
			// information _separately_ for the old and new index
			(isAdded ? 0 : 1), (isAdded ? 1 : 0),
			(isAdded ? 0 : avgBucketLength), (isAdded ? avgBucketLength : 0),
			(isAdded ? 0 : maxBucketLength), (isAdded ? maxBucketLength : 0)
		);
	}

	/**
	 * method that computes the change information between two buckets (changed).
	 * 
	 * @param newBucket the new bucket
	 * @param oldBucket the old bucket
	 * @return the change information
	 */
	private BucketsChangeInformation computeBucketChangeInformation(Bucket newBucket, Bucket oldBucket) {
		// changedBucketCountRatio is a measure for the changed number of items in a bucket.
		// formular:	max(count_new, count_old)
		//				-------------------------
		//				min(count_new, count_old)
		double minValue = Math.min(newBucket.getCount(), oldBucket.getCount());
		double maxValue = Math.max(newBucket.getCount(), oldBucket.getCount());
		if (minValue == 0.0) {
			throw new IllegalArgumentException("both bucket count must not be 0!");
		}
		double changedBucketCountRatio = maxValue / minValue;
		// bucketChanged contains the state whether the bucket was changed
		boolean bucketChanged = (newBucket.getCount() != oldBucket.getCount());
		
		// iterate over all dimensions
		int[] newLowerBoundaries = newBucket.getLowerBoundaries();
		int[] newUpperBoundaries = newBucket.getUpperBoundaries();
		int[] oldLowerBoundaries = oldBucket.getLowerBoundaries();
		int[] oldUpperBoundaries = oldBucket.getUpperBoundaries();
		int nmbOfDimensions = newLowerBoundaries.length;
		double newAvgBucketLength = 0.0; // the average length of the new bucket
		double oldAvgBucketLength = 0.0; // the average length of the old bucket
		double newMaxBucketLength = 0.0; // the maximum length of the new bucket
		double oldMaxBucketLength = 0.0; // the maximum length of the old bucket
		for (int i = 0; i < nmbOfDimensions; i++) {
			double currentNewLength = newUpperBoundaries[i] - newLowerBoundaries[i] + 1;
			double currentOldLength = oldUpperBoundaries[i] - oldLowerBoundaries[i] + 1;
			newAvgBucketLength += currentNewLength;
			oldAvgBucketLength += currentOldLength;
			if (currentNewLength > newMaxBucketLength) {
				newMaxBucketLength = currentNewLength;
			}
			if (currentOldLength > oldMaxBucketLength) {
				oldMaxBucketLength = currentOldLength;
			}
			// is there a change in the dimension's boundaries?
			if (oldUpperBoundaries[i] != newUpperBoundaries[i]) {
				bucketChanged = true;
			}
			if (oldLowerBoundaries[i] != newLowerBoundaries[i]) {
				bucketChanged = true;
			}
		}
		newAvgBucketLength = newAvgBucketLength / nmbOfDimensions;
		oldAvgBucketLength = oldAvgBucketLength / nmbOfDimensions;
		
		// changed*BucketLengthRatio is a measure for the changed bucket size.
		// formular:	max(length_new, length_old)
		//				---------------------------
		//				min(length_new, length_old)
		minValue = Math.min(newAvgBucketLength, oldAvgBucketLength);
		maxValue = Math.max(newAvgBucketLength, oldAvgBucketLength);
		if (minValue == 0.0) {
			throw new IllegalArgumentException("bucket length must not be 0!");
		}
		double changedAvgBucketLengthRatio = maxValue / minValue;

		minValue = Math.min(newMaxBucketLength, oldMaxBucketLength);
		maxValue = Math.max(newMaxBucketLength, oldMaxBucketLength);
		if (minValue == 0.0) {
			throw new IllegalArgumentException("bucket length must not be 0!");
		}
		double changedMaxBucketLengthRatio = maxValue / minValue;

		return new BucketsChangeInformation(
			// information _together_ for the old and new index
			1, (bucketChanged ? 1 : 0), changedBucketCountRatio,
			changedAvgBucketLengthRatio, changedMaxBucketLengthRatio,
			// information _separately_ for the old and new index
			1, 1, oldAvgBucketLength, newAvgBucketLength,
			oldMaxBucketLength, newMaxBucketLength
		);
	}

	/**
	 * method that determines if there is a change between two buckets.
	 * 
	 * @param newBucket the new bucket
	 * @param oldBucket the old bucket
	 * @return <tt>true</tt> if changed, <tt>false</tt> otherwise
	 */
	private boolean hasBucketChanged(Bucket newBucket, Bucket oldBucket) {
		// is there a change in the bucket count?
		if (newBucket.getCount() != oldBucket.getCount()) {
			return true;
		}

		// is there a change in the dimension's boundaries?
		int[] newLowerBoundaries = newBucket.getLowerBoundaries();
		int[] newUpperBoundaries = newBucket.getUpperBoundaries();
		int[] oldLowerBoundaries = oldBucket.getLowerBoundaries();
		int[] oldUpperBoundaries = oldBucket.getUpperBoundaries();
		for (int i = 0; i < newLowerBoundaries.length; i++) {
			if (oldUpperBoundaries[i] != newUpperBoundaries[i]) {
				return true;
			}
			if (oldLowerBoundaries[i] != newLowerBoundaries[i]) {
				return true;
			}
		}

		return false;
	}
}
