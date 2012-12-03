/*
 *
 */
package de.ilmenau.datasum.index.histogram;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

import javax.swing.tree.DefaultMutableTreeNode;

import de.ilmenau.datasum.config.Parameters;
import de.ilmenau.datasum.index.Bucket;
import de.ilmenau.datasum.index.BucketBasedLocalIndex;
import de.ilmenau.datasum.index.IntersectionInformation;
import de.ilmenau.datasum.index.LocalIndex;
import de.ilmenau.datasum.index.QuerySpace;
import de.ilmenau.datasum.index.SelectConstraint;
import de.ilmenau.datasum.index.update.UpdateBucket;
import de.ilmenau.datasum.util.DataPoint;


/**
 * This class represents a multidimensional histogram.
 * 
 * @author Christian Lemke
 * @author Katja Hose
 * @version $Id: MultiDimHistogram.java,v 1.24 2007-05-23 10:15:36 lemke Exp $
 */
@SuppressWarnings({"serial", "nls" })
public class MultiDimHistogram extends BucketBasedLocalIndex {
	/** the name of the node that elements ({@link #attributeNames}) are indexed */
	private String parentNodeName = null;
	/** attribute names of the dimensions which the histogram is defined on */
	private Vector<String> attributeNames;
	/** number of dimensions that this index is defined on */
	private int nmbOfDimensions;
	/** number of buckets for each dimension */
	private int nmbOfBucketsPerDimension;
	/** minimum extensions (boundaries) of the attributes (dimensions) */
	private int[] dimSpecMin;
	/** maximum extensions (boundaries) of the attributes (dimensions) */
	private int[] dimSpecMax;

	/** the ID of the peer where the corresponding index is located */
	private String peerID;
	/** the ID of the neighbor (<tt>null</tt> if the index is not part of a routing index) */
	private String neighborID;

	/** contains all lower boundaries of all buckets, [dimension][nmbOfBuckets] */
	private int[][] lowerBucketBoundsMatrix;
	/** contains all upper boundaries of all buckets, [dimension][nmbOfBuckets] */
	private int[][] upperBucketBoundsMatrix;

	/** array of buckets that belong to this multidimensional histogram */
	private MultiDimBucket[] buckets;

	/** the number of items in the index */
	private double nmbOfItemsInIndex;
	/** the number of buckets in the index */
	private int nmbOfBucketsInIndex;
	
	/** parameter indicating whether counts for each source should be kept (true) 
	 *    or counts for all contained sources altogether 
	 */
	private boolean storeDetailedCounts = true;


	/**
	 * compares the definitions of the two histograms.
	 * 
	 * @param hist1 first histogram
	 * @param hist2 second histogram
	 * @return <tt>true</tt> if both have the same definitions
	 */
	public static boolean compareDefinitions(MultiDimHistogram hist1, MultiDimHistogram hist2) {

		if (hist1.dimSpecMax.length != hist2.dimSpecMax.length) {
			System.out.println(
				"MultiDimRoutingHistogram.compareDefinitions(): sizes of dimSpecMax not equal, hist1="
				+ hist1.dimSpecMax.length + ", hist2=" + hist2.dimSpecMax.length
			);
			return false;
		}

		for (int i = 0; i < hist1.dimSpecMax.length; i++) {
			if (hist1.dimSpecMax[i] != hist2.dimSpecMax[i]) {
				System.out.println(
					"MultiDimRoutingHistogram.compareDefinitions(): values of dimSpecMax["
					+ i + "] not equal, hist1["	+ i	+ "]=" + hist1.dimSpecMax[i] + ", hist2[" + i
					+ "]=" + hist2.dimSpecMax[i]
				);
				return false;
			}
		}

		if (hist1.dimSpecMin.length != hist2.dimSpecMin.length) {
			System.out.println(
				"MultiDimRoutingHistogram.compareDefinitions(): sizes of dimSpecMin not equal, hist1="
				+ hist1.dimSpecMin.length + ", hist2=" + hist2.dimSpecMin.length
			);
			return false;
		}

		for (int i = 0; i < hist1.dimSpecMin.length; i++) {
			if (hist1.dimSpecMin[i] != hist2.dimSpecMin[i]) {
				System.out.println(
					"MultiDimRoutingHistogram.compareDefinitions(): values of dimSpecMin["
					+ i	+ "] not equal, hist1["	+ i	+ "]=" + hist1.dimSpecMin[i] + ", hist2[" + i
					+ "]=" + hist2.dimSpecMin[i]
				);
				return false;
			}
		}

		if (hist1.attributeNames.size() != hist2.attributeNames.size()) {
			System.out.println("MultiDimRoutingHistogram.compareDefinitions(): sizes of indexOnDimensions not equal, hist1="
				+ hist1.attributeNames.size() + ", hist2=" + hist2.attributeNames.size()
			);
			return false;
		}

		for (String currString : hist1.attributeNames) {
			if (!hist2.attributeNames.contains(currString)) {
				System.out.println(
					"MultiDimRoutingHistogram.compareDefinitions(): index dimension "
					+ currString + " is contained in hist1 but not in hist2"
				);
				return false;
			}
		}

		// TODO: genauer vergleichen + auch Inhalt
		if (hist1.lowerBucketBoundsMatrix.length != hist2.lowerBucketBoundsMatrix.length) {
			System.out.println(
				"MultiDimRoutingHistogram.compareDefinitions(): lowerBucketBoundsMatrix "
				+ "has not the same length in both histograms "
			);
			return false;
		}

		// TODO: genauer vergleichen + auch Inhalt
		if (hist1.upperBucketBoundsMatrix.length != hist2.upperBucketBoundsMatrix.length) {
			System.out.println(
				"MultiDimRoutingHistogram.compareDefinitions(): upperBucketBoundsMatrix "
				+ "has not the same length in both histograms "
			);
			return false;
		}

		if (hist1.nmbOfBucketsPerDimension != hist2.nmbOfBucketsPerDimension) {
			System.out.println(
				"MultiDimRoutingHistogram.compareDefinitions(): nmbOfBucketsPerDimension not equal, hist1="
				+ hist1.nmbOfBucketsPerDimension + ", hist2=" + hist2.nmbOfBucketsPerDimension
			);
			return false;
		}

		if (hist1.nmbOfDimensions != hist2.nmbOfDimensions) {
			System.out.println(
				"MultiDimRoutingHistogram.compareDefinitions(): nmbOfDimensions not equal, hist1="
				+ hist1.nmbOfBucketsPerDimension + ", hist2=" + hist2.nmbOfBucketsPerDimension
			);
			return false;
		}

		// if there is no difference in the comparisons above - return true - indicating that the definitions are the same
		return true;
	}

	/**
	 * constructs MultiDimHistogram according to the given parameters
	 *  --> for creating local indexes
	 * 
	 * @param data indicates the data fragment that should be represented by this index
	 * @param attributeNames defines on which attributes should be indexed
	 * @param dimSpecMin minimum extensions (boundaries) of the attributes (dimensions)
	 * @param dimSpecMax maximum extensions (boundaries) of the attributes (dimensions)
	 * @param nmbOfBucketsPerDimension number of buckets for each dimension
	 * @param peerID the ID of the peer that this histogram belongs to as local index 
	 * 			--> necessary to build a globally unique bucket ID
	 * @return MultiDimHistogram
	 */
	/*public MultiDimHistogram createIndex(Element data, Vector<String> attributeNames,
			int[] dimSpecMin, int[] dimSpecMax, int nmbOfBucketsPerDimension, String peerID, boolean storeDetailedCounts) {

		int nmbOfDimensions = attributeNames.size();

		// index for current peer
		MultiDimHistogram LRI = new MultiDimHistogram(
			nmbOfDimensions, nmbOfBucketsPerDimension, dimSpecMin, dimSpecMax,
			peerID, null, attributeNames, storeDetailedCounts
		);

		for (Object currChildObject : data.getChildren()) {
			Element currChild = (Element) currChildObject;
			if (LRI.parentNodeName == null) {
				LRI.parentNodeName = currChild.getName();
			}

			// create a data item to insert
			int[] currElement = new int[nmbOfDimensions];
			byte count = 0;
			for (String attr : attributeNames) {
				currElement[count] = Integer.parseInt(currChild.getChildText(attr));
				count++;
			}
			try {
				LRI.insertDataItem(currElement);
			} catch (Exception e) {
				// ignore invalid data
			}
		}
		return LRI;
	}*/

	/**
	 * constructor.
	 * 
	 * @param nmbOfDimensions number of dimensions that this index is defined on
	 * @param nmbOfBucketsPerDimension number of buckets for each dimension
	 * @param dimSpecMin minimum extensions (boundaries) of the attributes (dimensions)
	 * @param dimSpecMax maximum extensions (boundaries) of the attributes (dimensions)
	 * @param peerID the ID of the peer where the corresponding index is located
	 * @param neighborID the ID of the neighbor (<tt>null</tt> if the index is not part of a routing index)
	 * @param attributeNames defines on which attributes should be indexed
	 */
	public MultiDimHistogram(int nmbOfDimensions, int buckets, int[] dimSpecMin,
			int[] dimSpecMax, String peerID, String neighborID, Vector<String> attributeNames, boolean storeDetailedCounts) {

		super(IndexType.MultiDimHistogram);
		this.nmbOfDimensions = nmbOfDimensions;
		
		this.nmbOfBucketsPerDimension = (int) Math.pow(buckets, 1/(double)3);
		
		this.dimSpecMin = dimSpecMin;
		this.dimSpecMax = dimSpecMax;
		this.peerID = peerID;
		this.neighborID = neighborID;
		this.attributeNames = attributeNames;
		this.nmbOfItemsInIndex = 0.0;
		
		this.storeDetailedCounts = storeDetailedCounts;

		this.lowerBucketBoundsMatrix = new int[nmbOfDimensions][nmbOfBucketsPerDimension];
		this.upperBucketBoundsMatrix = new int[nmbOfDimensions][nmbOfBucketsPerDimension];

		// initialization: calculate all bucket boundaries --> buckets are created on demand 
		for (int i = 0; i < nmbOfDimensions; i++) {
			int dist = dimSpecMax[i] - dimSpecMin[i] + 1;
			double currDist = (double) dist / nmbOfBucketsPerDimension;

			int currStart = dimSpecMin[i];
			for (int j = 0; j < nmbOfBucketsPerDimension; j++) {
				this.lowerBucketBoundsMatrix[i][j] = currStart;
				this.upperBucketBoundsMatrix[i][j] = (int) Math.round(currStart + currDist) - 1;
				if (j == nmbOfBucketsPerDimension - 1) {
					this.upperBucketBoundsMatrix[i][j] = dimSpecMax[i];
				}
				currStart = this.upperBucketBoundsMatrix[i][j] + 1;
			}

			assert (this.upperBucketBoundsMatrix[i][nmbOfBucketsPerDimension - 1] == dimSpecMax[i]);
		}
//		System.out.println( (int) Math.round(Math.pow(nmbOfBucketsPerDimension, nmbOfDimensions)));
		this.buckets = new MultiDimBucket[
		    (int) Math.round(Math.pow(nmbOfBucketsPerDimension, nmbOfDimensions))
		];
	}
	
	
	public boolean storeDetailedCounts(){
		return this.storeDetailedCounts;
	}
	

	/**
	 * method that adds the data of another index to this one
	 * 
	 * @param inputIndex
	 */
	public void addIndexData(MultiDimHistogram inputIndex) {
		// same structure?
		assert (compareDefinitions(this, inputIndex));
		this.parentNodeName = inputIndex.parentNodeName;

		for (int i = 0; i < this.buckets.length; i++) {
			// is something to add?
			if (inputIndex.buckets[i] != null) {
				// is there local data?
				if (this.buckets[i] == null) {
					if (this.storeDetailedCounts){
						this.buckets[i] = new MultiDimBucket(
								inputIndex.buckets[i].getLowerBoundaries(), inputIndex.buckets[i].getUpperBoundaries(),
								inputIndex.buckets[i].getCount(), this, this.peerID, this.neighborID, i, inputIndex.buckets[i].getSourceIDMap()
							);
					} else {
						this.buckets[i] = new MultiDimBucket(
								inputIndex.buckets[i].getLowerBoundaries(), inputIndex.buckets[i].getUpperBoundaries(),
								inputIndex.buckets[i].getCount(), this, this.peerID, this.neighborID, i, inputIndex.buckets[i].getSourceIDs()
							);
					}
					this.nmbOfBucketsInIndex++;
				} else {
					this.buckets[i].updateCount(inputIndex.buckets[i].getCount());
					if (this.storeDetailedCounts){
						this.buckets[i].addSourceIDsAndCount(inputIndex.buckets[i].getSourceIDMap());
					} else {
						this.buckets[i].addSourceIDs(inputIndex.buckets[i].getSourceIDs());
					}
				}

				// only for evaluation store the points in the bucket!
				if (Parameters.storeDataPointsInBuckets) {
					this.buckets[i].addPoints(inputIndex.buckets[i].getContainedPoints());
				}
			}
		}
	}

	/* (non-Javadoc)
	 * @see smurfpdms.index.LocalIndex#buildJSubTree(javax.swing.tree.DefaultMutableTreeNode)
	 */
	@Override
	public void buildJSubTree(DefaultMutableTreeNode parentNode) {
		DefaultMutableTreeNode newNode = new DefaultMutableTreeNode(
			"indexed attributes: " + this.attributeNames.toString()
		);
		parentNode.add(newNode);

		newNode = new DefaultMutableTreeNode(
			"number of buckets per dimension: "	+ this.nmbOfBucketsPerDimension
		);
		parentNode.add(newNode);

		newNode = new DefaultMutableTreeNode(
			"used number of buckets: " + this.nmbOfBucketsInIndex
		);
		parentNode.add(newNode);

		if (this.nmbOfBucketsInIndex > 0) {
			newNode = new DefaultMutableTreeNode(
				"number of indexed elements: " + this.nmbOfItemsInIndex
			);
			parentNode.add(newNode);
		}

		newNode = new DefaultMutableTreeNode("Buckets");
		parentNode.add(newNode);
		for (int i = 0; i < this.buckets.length; i++) {
			if (this.buckets[i] != null) {
				DefaultMutableTreeNode buckNode = new DefaultMutableTreeNode(
					this.buckets[i].toString()
				);
				newNode.add(buckNode);
			}
		}
	}

	/* (non-Javadoc)
	 * @see smurfpdms.index.Index#clone()
	 */
	@Override
	public Object clone() {
		MultiDimHistogram clone = (MultiDimHistogram) super.clone();

		// clone buckets
		clone.buckets = this.buckets.clone();
		for (int i = 0; i < this.buckets.length; i++) {
			if (this.buckets[i] != null) {
				clone.buckets[i] = (MultiDimBucket) this.buckets[i].clone();
				clone.buckets[i].setCorrespondingIndex(clone);
			}
		}

		return clone;
	}

	/* (non-Javadoc)
	 * @see smurfpdms.index.LocalIndex#createEmptyLocalIndexForNeighbor(smurfpdms.peer.Neighbor)
	 */
	//@Override
	//public LocalIndex createEmptyLocalIndexForNeighbor(Neighbor neighbor) {
	public LocalIndex createEmptyLocalIndexForNeighbor() {
		/*return new MultiDimHistogram(
			this.nmbOfDimensions, this.nmbOfBucketsPerDimension,
			this.dimSpecMin, this.dimSpecMax,
			neighbor.getPeerID(), neighbor.getNeighborID(), this.attributeNames
		);*/
		return new MultiDimHistogram(
				this.nmbOfDimensions, this.nmbOfBucketsPerDimension,
				this.dimSpecMin, this.dimSpecMax,
				"0", "1", this.attributeNames, this.storeDetailedCounts
			);
	}
	
	

	/* (non-Javadoc)
	 * @see smurfpdms.index.BucketBasedLocalIndex#getAllBucketsCloned(boolean)
	 */
	@Override
	public Vector<Bucket> getAllBuckets(boolean clone) {
		Vector<Bucket> clonedBuckets = new Vector<Bucket>();

		for (int i = 0; i < this.buckets.length; i++) {
			if (this.buckets[i] != null) {
				if (clone) {
					clonedBuckets.add((MultiDimBucket) this.buckets[i].clone());
				} else {
					clonedBuckets.add(this.buckets[i]);
				}
			}
		}

		return clonedBuckets;
	}

	/* (non-Javadoc)
	 * @see smurfpdms.index.BucketBasedLocalIndex#getAllBucketsAsHashMapGlobalID()
	 */
	@Override
	public HashMap<String, Bucket> getAllBucketsAsHashMapGlobalID() {
		HashMap<String, Bucket> bucketList = new HashMap<String, Bucket>();

		for (int i = 0; i < this.buckets.length; i++) {
			if (this.buckets[i] != null) {
				bucketList.put(this.buckets[i].getGlobalBucketID(), this.buckets[i]);
			}
		}

		return bucketList;
	}

	/* (non-Javadoc)
	 * @see smurfpdms.index.BucketBasedLocalIndex#getAllBucketsInQuerySpace(smurfpdms.index.QuerySpace)
	 */
	@Override
	public Vector<IntersectionInformation> getAllBucketsInQuerySpace(QuerySpace qs) {
		Vector<IntersectionInformation> result =  new Vector<IntersectionInformation>();
		
		// the simple variant: no constraints over the query space
		if (qs.hasNoRestrictions()) {
			for (int i = 0; i < this.buckets.length; i++) {
				if (this.buckets[i] != null) {
					result.add(new IntersectionInformation(this.buckets[i], this.buckets[i], 1.0));
				}
			}
		} else { // otherwise we must determine the buckets that are in the restricted query space
			int[] lowerBucketAddress = determineMultiDimBucketAddress(qs.getLowerBoundaries());
			int[] upperBucketAddress = determineMultiDimBucketAddress(qs.getUpperBoundaries());
			int distInFirstDimension = upperBucketAddress[0] - lowerBucketAddress[0];

			int[] currentAddress = lowerBucketAddress.clone();
			int linearizedAddress;
			Bucket currentBucket;
			boolean done = false;
	
			do {
				// iterate over buckets in the first dimension
				// -> save computation of linearizeBucketAddress()
				linearizedAddress = linearizeBucketAddress(currentAddress);
				for (int i = 0; i <= distInFirstDimension; i++) {
					currentBucket = this.buckets[linearizedAddress + i];
					if (currentBucket != null) {
						result.add(
							computeIntersectionInformation(currentBucket, qs)
						);
					}
				}
				// iterate over all dimensions except the first (if more than one exists)
				// --> the following code realize a simple counter
				if (this.nmbOfDimensions > 1) {
					for (int j = 1; j < this.nmbOfDimensions; j++) {
						currentAddress[j]++;
						if (currentAddress[j] > upperBucketAddress[j]) {
							if (j == this.nmbOfDimensions - 1) {
								// all buckets tested -> done
								done = true;
							} else {
								currentAddress[j] = lowerBucketAddress[j];
								// do not break and increase the value of the higher dimension (j+1)
							}
						} else {
							break; // the for-loop
						}
					}
				} else {
					done = true;
				}
			} while (!done);
		}
		// now return the buckets (if any)
		return result;
	}

	/* (non-Javadoc)
	 * @see smurfpdms.index.BucketBasedLocalIndex#getAttributeNames()
	 */
	@Override
	public Vector<String> getAttributeNames() {
		return this.attributeNames;
	}

	/* (non-Javadoc)
	 * @see smurfpdms.index.BucketBasedLocalIndex#getNmbOfBuckets()
	 */
	@Override
	public int getNmbOfBuckets() {
		return this.nmbOfBucketsInIndex;
	}

	/* (non-Javadoc)
	 * @see smurfpdms.index.BucketBasedLocalIndex#getNmbOfDimensions()
	 */
	@Override
	public int getNmbOfDimensions() {
		return this.nmbOfDimensions;
	}

	/* (non-Javadoc)
	 * @see smurfpdms.index.BucketBasedLocalIndex#getNmbOfIndexedItems()
	 */
	@Override
	public int getNmbOfIndexedItems() {
		return (int) Math.round(this.nmbOfItemsInIndex);
	}

	/**
	 * @return returns parentNodeName
	 */
	public String getParentNodeName() {
		return this.parentNodeName;
	}

	/* (non-Javadoc)
	 * @see smurfpdms.index.Index#getStateString()
	 */
	@Override
	public String getStateString() {
		StringBuffer sb = new StringBuffer();

		sb.append("Index-Type: ");
		sb.append(this.type);
		sb.append("\n   Dimensions: ");
		sb.append(this.nmbOfDimensions);
		sb.append("\n   Number of buckets per dimension: ");
		sb.append(this.nmbOfBucketsPerDimension);
		sb.append("\n   Current number of buckets: ");
		sb.append(this.nmbOfBucketsInIndex);
		sb.append("\n   Current number of indexed elements: ");
		if (this.nmbOfBucketsInIndex > 0) {
			sb.append(this.nmbOfItemsInIndex);
			sb.append("\n   Buckets:");
			// print out all buckets
			for (int i = 0; i < this.buckets.length; i++) {
				if (this.buckets[i] != null) {
					sb.append("\n      ");
					sb.append(this.buckets[i].toString());
				}
			}
		}
		sb.append("\n");

		return sb.toString();
	}

	/* (non-Javadoc)
	 * @see smurfpdms.index.Index#hasAccordingData(java.lang.String)
	 */
	@Override
	public boolean hasAccordingData(String expression) {
		// TODO: what to do if the index is empty?
		Vector<SelectConstraint> constraints = this.getSelectConstraintsFromXPath(expression);
		for (int i = 0; i < this.buckets.length; i++) {
			if (this.buckets[i] != null && this.buckets[i].matchConstraints(constraints)) {
				return true;
			}
		}
		return false;
	}

	/* (non-Javadoc)
	 * @see smurfpdms.index.BucketBasedLocalIndex#hasFixedBucketBoundaries()
	 */
	@Override
	public boolean hasFixedBucketBoundaries() {
		return true;
	}

	/* (non-Javadoc)
	 * @see smurfpdms.index.BucketBasedLocalIndex#insertBucket(smurfpdms.update.UpdateBucket)
	 */
	//@Override
	public void insertBucket(UpdateBucket bucket, boolean insertIntoExistingBucketIfPossible) throws Exception {
		// determine the bucket address
		int bucketAddress = (int) bucket.getLocalBucketID();
		
		// if there is no bucket then create one
		if (this.buckets[bucketAddress] == null) {
			if (this.storeDetailedCounts){
				this.buckets[bucketAddress] = new MultiDimBucket(
						bucket.getLowerBoundaries(), bucket.getUpperBoundaries(), bucket.getCount(), this,
						this.peerID, this.neighborID, bucketAddress, bucket.getSourceIDMap()
					);
			} else {
				this.buckets[bucketAddress] = new MultiDimBucket(
					bucket.getLowerBoundaries(), bucket.getUpperBoundaries(), bucket.getCount(), this,
					this.peerID, this.neighborID, bucketAddress, bucket.getSourceIDs()
				);
			}
			this.nmbOfBucketsInIndex++;
		} else {
			// add infos
			if (this.storeDetailedCounts){
				this.buckets[bucketAddress].addSourceIDsAndCount(bucket.getSourceIDMap());
			} else {
				this.buckets[bucketAddress].updateCount(bucket.getCount());
				this.buckets[bucketAddress].addSourceIDs(bucket.getSourceIDs());
			}
		}
		// only for evaluation store the points in the bucket!
		if (Parameters.storeDataPointsInBuckets) {
			this.buckets[bucketAddress].addPoints(bucket.getContainedPoints());
		}
	}

	/* (non-Javadoc)
	 * @see smurfpdms.index.BucketBasedLocalIndex#insertDataItem(int[])
	 */
	//@Override
	public void insertDataItem(int[] coordinates, String sourceID) throws Exception {
		
		int[] multiDimBucketAddress = determineMultiDimBucketAddress(coordinates);
		int bucketAddress = linearizeBucketAddress(multiDimBucketAddress);

		if (this.buckets[bucketAddress] == null) {
			// determine boundaries
			int[] currMin = new int[this.nmbOfDimensions];
			int[] currMax = new int[this.nmbOfDimensions];
			for (int i = 0; i < this.nmbOfDimensions; i++) {
				currMin[i] = this.lowerBucketBoundsMatrix[i][multiDimBucketAddress[i]];
				currMax[i] = this.upperBucketBoundsMatrix[i][multiDimBucketAddress[i]];
			}
			
			MultiDimBucket newBucket = null;
			if (this.storeDetailedCounts){
				HashMap<String,Double> sourceMap = new HashMap<String,Double>();
				sourceMap.put(sourceID, 1.0);
				
				newBucket = new MultiDimBucket(
					currMin, currMax, 1.0, this, this.peerID, this.neighborID, bucketAddress, sourceMap
				);
			} else{
				
				HashSet<String> sourceSet = new HashSet<String>();
				sourceSet.add(sourceID);
				
				newBucket = new MultiDimBucket(
						currMin, currMax, 1.0, this, this.peerID, this.neighborID, bucketAddress, sourceSet
					);
			}
			
			this.buckets[bucketAddress] = newBucket;
			this.nmbOfBucketsInIndex++;
		} else {
			//this.buckets[bucketAddress].updateCount(1.0);
			this.buckets[bucketAddress].updateCount(1.0, sourceID);
		}
		// only for evaluation store the points in the bucket!
		if (Parameters.storeDataPointsInBuckets) {
			this.buckets[bucketAddress].addPoint(
				new DataPoint(coordinates, this.parentNodeName, this.attributeNames)
			);
		}
	}

	/* (non-Javadoc)
	 * @see smurfpdms.index.Index#isEmpty()
	 */
	@Override
	public boolean isEmpty() {
		return this.nmbOfItemsInIndex < 0.5;
	}

	/* (non-Javadoc)
	 * @see smurfpdms.index.BucketBasedLocalIndex#removeBucket(smurfpdms.index.updates.UpdateBucket)
	 */
	@Override
	public void removeBucket(UpdateBucket bucket) throws Exception {
		// first test if the given bucket is in the index
		int bucketAddress = (int) bucket.getLocalBucketID();
		if (this.buckets[bucketAddress] == null) {
			throw new IllegalStateException("The given bucket is not in the index!");
		}
		
		// only for evaluation remove the points from the bucket!
		if (Parameters.storeDataPointsInBuckets) {
			this.buckets[bucketAddress].removePoints(bucket.getContainedPoints());
		}
		// if found then remove infos
		this.buckets[bucketAddress].updateCount(bucket.getCount() * -1.0);
	}

	/* (non-Javadoc)
	 * @see smurfpdms.index.BucketBasedLocalIndex#removeDataItem(int[])
	 */
	@Override
	public void removeDataItem(int[] coordinates) throws Exception {
		int[] multiDimBucketAddress = determineMultiDimBucketAddress(coordinates);
		int bucketAddress = linearizeBucketAddress(multiDimBucketAddress);

		if (this.buckets[bucketAddress] != null) {
			// only for evaluation store the points in the bucket!
			if (Parameters.storeDataPointsInBuckets) {
				this.buckets[bucketAddress].removePoint(
					new DataPoint(coordinates, this.parentNodeName, this.attributeNames)
				);
			}
			// after that update the counter
			this.buckets[bucketAddress].updateCount(-1.0);
		} else {
			throw new IllegalStateException("No bucket for the given data item found!");
		}
	}

	/**
	 * @param parentNodeName sets parentNodeName
	 */
	public void setParentNodeName(String parentNodeName) {
		this.parentNodeName = parentNodeName;
	}

	/* (non-Javadoc)
	 * @see smurfpdms.index.BucketBasedLocalIndex#updateBucket(smurfpdms.update.UpdateBucket)
	 */
	@Override
	public void updateBucket(UpdateBucket bucket) throws Exception {
		// first test if the given bucket is in the index
		int bucketAddress = (int) bucket.getLocalBucketID();
		if (this.buckets[bucketAddress] == null) {
			throw new IllegalStateException("The given bucket is not in the index! (" + bucket + ")");
		}
		
		// only for evaluation store the points in the bucket!
		if (Parameters.storeDataPointsInBuckets) {
			this.buckets[bucketAddress].addPoints(bucket.getContainedPoints());
 			this.buckets[bucketAddress].removePoints(bucket.getPointsToRemove());
		}
		// if found then update infos (boundaries cannot change!)
		this.buckets[bucketAddress].updateCount(bucket.getCount());
	}

	
	/**
	 * method that removes the bucket with the given address.
	 * (used by MultiDimBucket.updateCount)
	 * 
	 * @param bucketAddress the address of the bucket to remove
	 */
	void removeBucket(int bucketAddress) {
		this.buckets[bucketAddress] = null;
		this.nmbOfBucketsInIndex--;
	}
	
	/**
	 * method that updates the number of elements in this index.
	 * (used by MultiDimBucket.updateCount and by the constructor of MultiDimBucket)
	 * 
	 * @param amount how much to add (positive) or remove (negative)
	 */
	void updateNmbOfItemsInIndex(double amount) {
		this.nmbOfItemsInIndex += amount;
	}

	/**
	 * method that determines the position of the given element in the multidimensional bucket array.
	 * 
	 * @param dataItem the element
	 * @return the multidimensional bucket position
	 */
	private int[] determineMultiDimBucketAddress(int[] dataItem) {
		int[] bucketAddress = new int[dataItem.length];
//		System.out.println("DATA:"+Arrays.toString(dataItem));
//		// determine bucket address by comparing element with lower bucket borders
//		System.out.println("nb of dim:"+nmbOfDimensions);
//		System.out.println(nmbOfBucketsPerDimension);
		for (int i = 0; i < this.nmbOfDimensions; i++) {
			for (int j = 1; j < this.nmbOfBucketsPerDimension; j++) {
				int currentLowerBorder = this.lowerBucketBoundsMatrix[i][j];
//				System.out.println("CURR LOWER BOUND: "+currentLowerBorder);
				if (dataItem[i] < currentLowerBorder) {
					bucketAddress[i] = j - 1;
					break;
				} else if (dataItem[i] == currentLowerBorder) {
					bucketAddress[i] = j;
					break;
				}
				if (j == this.nmbOfBucketsPerDimension - 1) {
					bucketAddress[i] = j;
				}
			}
		}
//		System.out.println("BucketAddress: "+Arrays.toString(bucketAddress));
		return bucketAddress;
	}

	/**
	 * method that linearize the multidimensional bucket postion.
	 * 
	 * @param indexes the multidimensional indexes
	 * @return the postion in the (one dimensional) buckets array
	 */
	private int linearizeBucketAddress(int[] indexes) {
		int address = 0;

		for (int i = 0; i < indexes.length; i++) {
			address += indexes[i] * Math.round(Math.pow(this.nmbOfBucketsPerDimension, i));
		}

		return address;
	}

	public int getMaxBuckets() {
		return this.nmbOfBucketsPerDimension;
	}
}
