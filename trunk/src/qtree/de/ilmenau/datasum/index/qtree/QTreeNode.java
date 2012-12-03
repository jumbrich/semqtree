/**
 * 
 */
package de.ilmenau.datasum.index.qtree;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Vector;
import java.util.Map.Entry;

import javax.swing.tree.DefaultMutableTreeNode;

import de.ilmenau.datasum.config.Parameters;
import de.ilmenau.datasum.exception.QTreeException;
import de.ilmenau.datasum.index.Bucket;
import de.ilmenau.datasum.index.qtree.penalty.QPenaltyFunction;
import de.ilmenau.datasum.util.DataPoint;
import de.ilmenau.datasum.util.StringHelper;

/*
 * Implementation of Daniel Zinn`s QTree in Java
 * 
 */

/**
 * @author matz, hose
 * 
 * TODO<MM>: <DONE> unbenutzte/alte Parameter und Methoden entfernen
 */
public class QTreeNode extends Bucket {
	private static final long serialVersionUID = 1L;
	/** Reference to the owning QTree */
	protected QTree qtree;	

	/** reference to the parent node of this node, if it is null this is the root node of a QTree */
	protected QTreeNode parent;
	
	/** This Nodes Childrens List */
	protected Vector<QTreeNode> children;
	
	/** Number of Buckets currently are present as Children */
	protected int nmbOfCurrentBuckets;
	
	/** Reference to the next "QTree.getMergeCount" Buckets that would
	 *  be merged together
	 */
	protected Vector<QTreeNode> nextToMerge;
	/** Resulting penalty if the Buckets in nextToMerge are merged */
	protected double nextToMergePen;
	
	/** 
	 *  This Value is used for producing repeatable Results for Randomly
	 *  choosing a Set of Children that should be merged or grouped
	 */
	private static long currRandSeed=1523637124;

	/**
	 * Creates a new QTreeNode with the given Parameters
	 * 
	 * @param q - The Qtree this Node belongs to
	 * @param parent - The parent Node of the new created Node
	 * @param minBounds - The Minimum Boundaries
	 * @param maxBounds - The Maximum Boundaries
	 * @param count - The Number of Items that belong to this Node
	 * @param peerID - The ID of the Peer this Node belongs to
	 * @param neighborID - The Id of the Neighbor this Node belongs to
	 */
	public QTreeNode(QTree q, QTreeNode parent, 
			int[] minBounds, int[] maxBounds, double count, 
			String peerID, String neighborID, String sourceID){
		
		super(minBounds, maxBounds, count, peerID, neighborID, sourceID);
		
		this.parent = parent;
		this.qtree = q;
		this.nmbOfCurrentBuckets = 0;
		this.children = new Vector<QTreeNode>();
		this.nextToMerge = new Vector<QTreeNode>();
	}
	
	
	/**
	 * constructor used by QTreeBucket, QTreeNode.newShallowCopyForBounds()
	 * 
	 * @param q - The Qtree this Node belongs to
	 * @param parent - The parent Node of the new created Node
	 * @param minBounds - The Minimum Boundaries
	 * @param maxBounds - The Maximum Boundaries
	 * @param count - The Number of Items that belong to this Node
	 * @param peerID - Unique ID of the Peer this Node belongs to
	 * @param neighborID - Unique Id of the Neighbor this Node belongs to
	 * @param localBucketID - Unique ID within this QTree 
	 */
	public QTreeNode(QTree q, QTreeNode parent, 
			int[] minBounds, int[] maxBounds, double count, 
			String peerID, String neighborID, long localBucketID, 
			HashSet<String> sourceIDs){
		
		super(minBounds, maxBounds, count, peerID, neighborID, localBucketID, sourceIDs);
		
		this.parent = parent;
		this.qtree = q;
		this.nmbOfCurrentBuckets = 0;
		this.children = new Vector<QTreeNode>();
		this.nextToMerge = new Vector<QTreeNode>();
	}
	
	/**
	 * constructor used by QTreeBucket, QTreeNode.newShallowCopyForBounds()
	 * 
	 * @param q - The Qtree this Node belongs to
	 * @param parent - The parent Node of the new created Node
	 * @param minBounds - The Minimum Boundaries
	 * @param maxBounds - The Maximum Boundaries
	 * @param count - The Number of Items that belong to this Node
	 * @param peerID - Unique ID of the Peer this Node belongs to
	 * @param neighborID - Unique Id of the Neighbor this Node belongs to
	 * @param localBucketID - Unique ID within this QTree 
	 */
	public QTreeNode(QTree q, QTreeNode parent, 
			int[] minBounds, int[] maxBounds, double count, 
			String peerID, String neighborID, long localBucketID, 
			HashMap<String,Double> sourceIDMap){
		
		super(minBounds, maxBounds, count, peerID, neighborID, localBucketID, sourceIDMap);
		
		this.parent = parent;
		this.qtree = q;
		this.nmbOfCurrentBuckets = 0;
		this.children = new Vector<QTreeNode>();
		this.nextToMerge = new Vector<QTreeNode>();
	}
	
	
	/**
	 * constructor used by QTreeBucket, QTreeNode.newShallowCopyForBounds()
	 * 
	 * @param q - The Qtree this Node belongs to
	 * @param parent - The parent Node of the new created Node
	 * @param minBounds - The Minimum Boundaries
	 * @param maxBounds - The Maximum Boundaries
	 * @param count - The Number of Items that belong to this Node
	 * @param peerID - Unique ID of the Peer this Node belongs to
	 * @param neighborID - Unique Id of the Neighbor this Node belongs to
	 * @param localBucketID - Unique ID within this QTree 
	 */
	public QTreeNode(QTree q, QTreeNode parent, 
			int[] minBounds, int[] maxBounds, 
			String peerID, String neighborID, long localBucketID, 
			HashMap<String,Double> sourceCounts){
		
		super(minBounds, maxBounds, peerID, neighborID, localBucketID, sourceCounts);
		
		this.parent = parent;
		this.qtree = q;
		this.nmbOfCurrentBuckets = 0;
		this.children = new Vector<QTreeNode>();
		this.nextToMerge = new Vector<QTreeNode>();
	}
	
	/**
	 * Constructor for creating an inner node with a set of more
	 * than 2 children
	 * 
	 * @param qtree
	 * @param parent
	 * @param children
	 */
	public QTreeNode(QTree qtree, QTreeNode parent, Vector<QTreeNode> children) {
		this.count = 0.0;
		this.children = new Vector<QTreeNode>();
		// Important ! Don't clone anything !
		for (QTreeNode q : children){
			this.children.add(q);
			if (!qtree.storeDetailedCounts()) {
				this.addSourceIDs(q.getSourceIDs());
			}
		}
		this.qtree = qtree;
		this.parent = parent;
		this.lowerBoundaries = null;
		this.upperBoundaries = null;
		
		for (QTreeNode q : this.children) {
			q.parent = this;
			
			this.enlargeBounds(q);
			
			if (!qtree.storeDetailedCounts()){
				this.addSourceIDsAndCount(q.getSourceIDMap());
			} else {
				this.count += q.count;
				this.addSourceIDs(q.getSourceIDs());
			}
		}
		// this.degree = 2;
		//add node to priority Queue
		// updatePQ();
		this.nmbOfCurrentBuckets = 0;	// ?
		
	}
	
	/**
	 * Checks whether the coordinates are inside this Nodes Boundaries   
	 * 
	 * @param coordinates Array with one Value for each Dimension 
	 * @return True if the coordinates are situated within this Node
	 * @throws QTreeException 
	 */
	protected boolean isResponsibleFor(int[] coordinates) throws QTreeException{
		if (this.lowerBoundaries == null || this.upperBoundaries == null){ // ... nur bei der Root Node
			if (this == this.qtree.getRoot()) return true; // vielleicht das hier verwenden?			
			throw new QTreeException("minBound = null @ other than RootNode");
		} else {
			for (int i=0;i<qtree.getNmbOfDimensions();i++){
				if (coordinates[i] < this.lowerBoundaries[i] || coordinates[i] > this.upperBoundaries[i]){
					return false;
				}
			}
		}		
		return true;
	}

	/**
	 * CHECKED
	 * 
	 * 
	 * Checks whether a Buckets Coordinates (<emph>q</emph>) are completely contained in this Node's Boundaries (<emph>this</emph>)
	 * 
	 * TODO<MM> DONE wie beim Einfuegen von Punkten, den Fall betrachten, dass der QTree leer 
	 * ist und das Bucket an der RootNode eingefuegt werden muss
	 * 
	 * @param q The Bucket to Check
	 * @return True if the Bucket's inside this Node
	 * @throws QTreeException 
	 */
	protected boolean isResponsibleFor(QTreeBucket q) throws QTreeException{
		if (this.lowerBoundaries == null || this.upperBoundaries == null){ // only possible at root node
			if (this == this.qtree.getRoot()) return true; 		
			throw new QTreeException("minBound = null @ other than RootNode");
		} else {
			
			for (int i=0;i<this.qtree.getNmbOfDimensions();i++){
				if ( (q.upperBoundaries[i] > this.upperBoundaries[i]) || (q.lowerBoundaries[i] < this.lowerBoundaries[i]) ) 
					return false;
			}
			
		}
		return true;
	}
	
	
	/**
	 * Checks whether a Node (<emph>this</emph>) overlaps another node (<emph>q</emph>)
	 * 
	 * 
	 * @param q node the check overlap with
	 * @return true if the two buckets overlap
	 * 
	 * @throws QTreeException 
	 */
	protected boolean overlaps(QTreeBucket q) throws QTreeException{
		boolean overlaps = true;
		if (this.lowerBoundaries == null || this.upperBoundaries == null){ // only possible at root node
			if (this == this.qtree.getRoot()) return true; 		
			throw new QTreeException("minBound = null @ other than RootNode");
		} else {
			for (int i=0;i<this.qtree.getNmbOfDimensions();i++){
				if (!( (q.upperBoundaries[i] >= this.lowerBoundaries[i]) && (q.lowerBoundaries[i] <= this.upperBoundaries[i])) ) 
					return false;
			}
			
		}
		return overlaps;
	}
	
	/**
	 * propagate sourceID upwards within the tree : the sourceID is added 
	 * to all nodes on the path between the current node and the root node.
	 * @param sourceID
	 */
	protected void propagateSourceIDUpwardsInTree(String sourceID){
		this.addSourceID(sourceID, this.qtree.storeDetailedCounts());
		if (this != this.qtree.getRoot() && this.parent != null){
			this.parent.addSourceID(sourceID, this.qtree.storeDetailedCounts());
		}
	}
	
	/**
	 * propagate sourceID upwards within the tree : the sourceID is added 
	 * to all nodes on the path between the current node and the root node.
	 * @param sourceID
	 */
	protected void propagateSourceIDUpwardsInTree(HashMap<String,Double> sourceIMapToAdd){
		this.addSourceIDsAndCount(sourceIMapToAdd);
		if (this != this.qtree.getRoot() && this.parent != null){
			this.parent.propagateSourceIDUpwardsInTree(sourceIMapToAdd);
		}
	}
	
	
	/**
	 * CHECKED: 
	 * 
	 * Inserts a single multidimensional data point
	 * 
	 * @param coordinates the coordinates of the point
	 * @throws Exception 
	 */
	protected void insertDataItem (int [] coordinates, String sourceID) throws Exception {
		
		// if this method is called the first time, i.e., at the root node, then check first, 
		// if there are any buckets that completely contain the coordinates
		// if such buckets exist, insert the coordinates into the most responsible one and return
		if (this == qtree.getRoot()){
			if (!this.qtree.storeDetailedCounts()){
				this.addSourceID(sourceID, this.qtree.storeDetailedCounts());
			}
			
			QTreeBucket mostResponsible = getMostResponsibleBucket(coordinates, sourceID);
			if (mostResponsible != null){
				
				// increase the counter
				if (!this.qtree.storeDetailedCounts()){
					// do not regard any sourceIDs at all
					mostResponsible.updateCount(1);
						
					// add sourceID
					if (sourceID!=null){
						//mostResponsible.addSourceID(sourceID); // already done by the method underneath
						mostResponsible.propagateSourceIDUpwardsInTree(sourceID);
					}
				} else {
					//mostResponsible.updateCount(1,sourceID);
					//this.addSourceID(sourceID, this.qtree.storeDetailedCounts());
					mostResponsible.propagateSourceIDUpwardsInTree(sourceID);
				}


				// only for evaluation store the points in the bucket!
				if (Parameters.storeDataPointsInBuckets) {
					mostResponsible.addPoint(
						new DataPoint(coordinates, this.qtree.getParentNodeName(), this.qtree.getAttributeNames())
					);
				}
				
				return;
			}
		}
		
		
		QTreeNode c = getMostResponsibleChild(coordinates);
		if (c != null) { 
			this.addSourceID(sourceID, this.qtree.storeDetailedCounts());
			c.insertDataItem(coordinates, sourceID); 
			
		} else {
			
			// add record as a new child bucket
			QTreeBucket newBucket = null;
			if (!this.qtree.storeDetailedCounts()){
				HashSet<String> sourceIDs = new HashSet<String>();
				sourceIDs.add(sourceID);
				
				newBucket = new QTreeBucket(this.qtree, this, coordinates.clone(), 
						coordinates.clone(), 1, this.qtree.getPeerID(), this.qtree.getNeighborID(), 
						this.qtree.getQTreeUniqueBucketID(), sourceIDs);
			} else {
				HashMap<String, Double> storeCount = new HashMap<String, Double>();
				storeCount.put(sourceID,new Double(1));
				
				newBucket = new QTreeBucket(this.qtree, this, coordinates.clone(), 
						coordinates.clone(), this.qtree.getPeerID(), this.qtree.getNeighborID(), 
						this.qtree.getQTreeUniqueBucketID(), storeCount);
				
			}
			
			// only for evaluation store the points in the bucket!
			if (Parameters.storeDataPointsInBuckets) {
				newBucket.addPoint(
					new DataPoint(coordinates, this.qtree.getParentNodeName(), this.qtree.getAttributeNames())
				);
			}
			
			children.add(newBucket);
			
			this.qtree.incCurrBuckets(); 	//  root.cntBuckets++
			if (!this.qtree.storeDetailedCounts()){
				this.count++;
			}
			this.addSourceID(sourceID, this.qtree.storeDetailedCounts());
 
			this.enlargeBounds(newBucket);
			if (this.children.size() > this.qtree.getMaxDegree()) {			
				this.reduceFanout(newBucket);
			}
			else {
				this.updatePrioQueue();
			}
 
			this.qtree.reduceBuckets();
		}
	}
	
	
	/**
	 * CHECKED:
	 * 
	 * Inserts a whole Bucket
	 * 
	 * @param q The Bucket to insert
	 * @param setNewLocalBucketID Set to true if you want that a new UniqueBucketId should be assigned
	 * 							  to this Bucket otherwise false
	 * @throws QTreeException 
	 */
	protected void insertBucket(QTreeBucket q, boolean setNewLocalBucketID, 
			boolean insertIntoExistingBucketIfPossible) throws QTreeException{
		
		addSourceIDs(q.getSourceIDs());
		
		// if this method is called the first time, i.e., at the root node, then check first, 
		// if there are any buckets that completely contain the bucket (q) to insert
		// if such buckets exist, insert the bucket (q) into the most responsible one and return
		if (insertIntoExistingBucketIfPossible && this == qtree.getRoot()){
			
			//this.addSourceIDs(q.getSourceIDs());
			
			QTreeBucket mostResponsible = getMostResponsibleBucket(q);
			if (mostResponsible != null){
				// increase the counter
				mostResponsible.updateCount(q.getCount());
				
				// add source IDs
				//mostResponsible.addSourceIDs(q.getSourceIDs());
				
				// add sourceIDs
				if (!q.getSourceIDs().isEmpty()){
					for (String sourceID: q.getSourceIDs()){
						mostResponsible.propagateSourceIDUpwardsInTree(sourceID);
					}
				}
				if (this.qtree.storeDetailedCounts()){
					mostResponsible.propagateSourceIDUpwardsInTree(q.getSourceIDMap());
				}

				// only for evaluation store the points in the bucket!
				if (Parameters.storeDataPointsInBuckets) {
					mostResponsible.addPoints(q.getContainedPoints());
				}
				return;
			}
		}
		
		// should have been outsourced to the class QTreeBucket -- this cannot be reached, can it?
		if (this instanceof QTreeBucket && this.completelyEncloses(q)) {
			
			if (!this.qtree.storeDetailedCounts()){
				this.count = this.count + q.count;
			
				((QTreeBucket) this).addSourceIDs(q.getSourceIDs());
			} else {
				this.addSourceIDsAndCount(q.getSourceIDMap());
			}
			
			// For Evaluation
			if (Parameters.storeDataPointsInBuckets) {
				this.addPoints(q.getContainedPoints());
			}
			
			return;
		}
		
		
		QTreeNode c = getMostResponsibleChildBucket(q);
		if (	(c != null && insertIntoExistingBucketIfPossible) || 
				(!insertIntoExistingBucketIfPossible && c != null && !(c instanceof QTreeBucket)) 
			){
			if (!this.qtree.storeDetailedCounts()){
				this.count = this.count + q.count;
			} else {
				this.addSourceIDsAndCount(q.getSourceIDMap());
			}
		
			c.insertBucket(q,setNewLocalBucketID,insertIntoExistingBucketIfPossible); 
		}
		else {
			
			QTreeBucket nb = null;
			if (setNewLocalBucketID){
				if (!this.qtree.storeDetailedCounts()){
					nb = new QTreeBucket(this.qtree, this, q.lowerBoundaries.clone(), q.upperBoundaries.clone(), 
							q.count, this.qtree.getPeerID(), this.qtree.getNeighborID(), this.qtree.getQTreeUniqueBucketID(),
							q.getSourceIDs());
				} else {
					nb = new QTreeBucket(this.qtree, this, q.lowerBoundaries.clone(), q.upperBoundaries.clone(), 
							q.count, this.qtree.getPeerID(), this.qtree.getNeighborID(), this.qtree.getQTreeUniqueBucketID(),
							q.getSourceIDMap());
				}

			} else {
				if (!this.qtree.storeDetailedCounts()){
					nb = new QTreeBucket(this.qtree, this, q.lowerBoundaries.clone(), q.upperBoundaries.clone(), 
						q.count, this.qtree.getPeerID(), this.qtree.getNeighborID(), q.getLocalBucketID(), q.getSourceIDs());
				} else {
					nb = new QTreeBucket(this.qtree, this, q.lowerBoundaries.clone(), q.upperBoundaries.clone(), 
							q.count, this.qtree.getPeerID(), this.qtree.getNeighborID(), q.getLocalBucketID(), 
							q.getSourceIDMap());					
				}
				if (q.getGlobalBucketID() != null){
					nb.setGlobalBucketID(q.getGlobalBucketID());
				}
			}
			
			// For Evaluation
			if (Parameters.storeDataPointsInBuckets) {
				nb.addPoints(q.getContainedPoints());
			}
			
			this.children.add(nb); 
			
			this.qtree.incCurrBuckets(); 	//  root.cntBuckets++
			if (this.qtree.storeDetailedCounts()){
				this.count += nb.count;
				this.addSourceIDs(nb.getSourceIDs());
			} else {
				this.addSourceIDsAndCount(nb.getSourceIDMap());
			}
		    
			this.enlargeBounds(nb);		
			
		
			if (this.children.size() > this.qtree.getMaxDegree()) {
				this.reduceFanout(nb);
			}
			else {
				this.updatePrioQueue();
			}
			
			// call reduceBuckets in any case --> doesn't matter since that method 
			// checks the number of buckets again and does nothing if there aren't too many buckets
			// It also makes sure that if this QTree belongs to a Routing Index, the reduceBuckets
			// Method of the Routing Index would be called
			this.qtree.reduceBuckets();			
		}
	}
	
	
	/**
	 * Method that returns true when <i>node</i>'s MBB is completely enclosed in this node's MBB
	 * @param node
	 * @return True if this node completely encloses <i>node</i>
	 */
	protected boolean completelyEncloses(QTreeNode node){
		
		// means if this is the root node
		if (this.lowerBoundaries==null || this.upperBoundaries==null){
			return true;
		}
		
		for (int i=0;i<this.qtree.getNmbOfDimensions();i++){
			if (this.lowerBoundaries[i] > node.lowerBoundaries[i])
				return false;
			if (this.upperBoundaries[i] < node.upperBoundaries[i])
				return false;
		}
		
		return true;
	}
	
	
	/**
	 * CHECKED:
	 * 
	 * Returns the Most Responsible Child for the given set of coordinates
	 * or NULL if there is no responsible child
	 * 
	 * @param coords The coordinates of the data point
	 * @return The Node that is most responsible for the datapoint <i>coords</i>
	 * @throws QTreeException 
	 */
	protected QTreeNode getMostResponsibleChild(int [] coords) throws QTreeException{
		// <TODO>(DONE) Falls es mehrere RespChilds mit gleichem cf gibt Zufallsauswahl
		if (this.children == null) return null;
		
		QTreeNode [] tempResultList;
		// if (this.children.size()< 2) return null; // ??
//		if (this.children.size()==1) {
//			if () returnthis.children.firstElement();
//		}
		if (this.children.size()==0) return null;
		tempResultList = new QTreeNode [this.children.size()];
		tempResultList[0] = null; // if nothings found return null
		
		int tempResultListTop = 0;
		double topPenalty = Double.MAX_VALUE;
		for (QTreeNode cur:this.children) {
			if (cur.isResponsibleFor(coords)) {
				double curcf = cur.getDistanceToNodeCenter(coords);				
				if (curcf < topPenalty) {
					topPenalty = curcf;
					tempResultListTop=0; // if we had another set of values before
					tempResultList[tempResultListTop] = cur; // The Top Element has the lowest Penalty 
				} else
				if (curcf == topPenalty) {     // More than one with same pen
					tempResultListTop++;               // Next Element in ResultList
					tempResultList[tempResultListTop] = cur;      // Save in tRL
				}
			}
		}
		// if there are more than one minimum penalty Nodes
		// choose one by Random
		if (tempResultListTop > 0) { 
			Random rnd = new Random(currRandSeed);
			int x = rnd.nextInt(tempResultListTop+1);
			return tempResultList[x];
		} else {
			return tempResultList[0];
		}
	}
	
	
	/**
	 * 
	 * Returns the Most Responsible Bucket of the QTree for the given set of coordinates
	 * or NULL if there is no responsible bucket
	 * 
	 * @param coords The coordinates of the data point
	 * @return The Node that is most responsible for the datapoint <i>coords</i>
	 * @throws QTreeException 
	 */
	protected QTreeBucket getMostResponsibleBucket(int [] coords, String sourceID) throws QTreeException{
		Vector<QTreeBucket> buckets = this.qtree.getAllBuckets();
		
		if (buckets == null) return null;
		if (buckets.size() == 0) return null;
		
		QTreeBucket [] tempResultList;
		tempResultList = new QTreeBucket [buckets.size()];
		tempResultList[0] = null; // if nothings found return null
		
		int tempResultListTop = 0;
		double topPenalty = Double.MAX_VALUE;
		for (QTreeBucket cur:buckets) {
			if (cur.isResponsibleFor(coords)) {
				double curcf = cur.getDistanceToNodeCenter(coords);				
				if (curcf < topPenalty) {
					topPenalty = curcf;
					tempResultListTop=0; // if we had another set of values before
					tempResultList[tempResultListTop] = cur; // The Top Element has the lowest Penalty 
				} else
				if (curcf == topPenalty) {     // More than one with same pen
					tempResultListTop++;               // Next Element in ResultList
					tempResultList[tempResultListTop] = cur;      // Save in tRL
				}
			}
		}
		// if there are more than one minimum penalty Nodes
		// choose one by Random if sourceIDs are not provided
		// otherwise, change the one that already contains the sourceID
		if (tempResultListTop > 0) {
			if (buckets.firstElement().getSourceIDs().isEmpty()){
				Random rnd = new Random(currRandSeed);
				int x = rnd.nextInt(tempResultListTop+1);
				return tempResultList[x];
			} else {
				for (int i=0;i<tempResultListTop+1;i++){
					if (!this.qtree.storeDetailedCounts()){
						if (tempResultList[i].getSourceIDs().contains(sourceID)){
							return tempResultList[i];
						}
					} else {
						if (tempResultList[i].getSourceIDMap().keySet().contains(sourceID)){
							return tempResultList[i];
						}						
					}
				}
				//if there is no bucket already containing the sourceID, return a random bucket
				Random rnd = new Random(currRandSeed);
				int x = rnd.nextInt(tempResultListTop+1);
				return tempResultList[x];
			}
		} else {
			return tempResultList[0];
		}
		
	}
	
	
	/**
	 * 
	 * Returns the Most Responsible Bucket of the QTree for the given bucket
	 * or NULL if there is no responsible bucket
	 * 
	 * @param delBucket Bucket whose points shall be deleted
	 * @return The node that is most responsible for the bucket <i>delBucket</i>
	 * @throws QTreeException 
	 */
	protected QTreeBucket getMostResponsibleBucket(QTreeBucket delBucket){
		Vector<QTreeBucket> buckets = this.qtree.getAllBuckets();
		
		if (buckets == null) return null;
		if (buckets.size() == 0) return null;
		
		QTreeBucket [] tempResultList;
		tempResultList = new QTreeBucket [buckets.size()];
		tempResultList[0] = null; // if nothings found return null
		
		int tempResultListTop = 0;
		double topPenalty = Double.MAX_VALUE;
		for (QTreeBucket cur:buckets) {
			if (cur.completelyEncloses(delBucket)) {
				double curcf = cur.getDistanceBucketCenterToNodeCenter(delBucket);				
				if (curcf < topPenalty) {
					topPenalty = curcf;
					tempResultListTop=0; // if we had another set of values before
					tempResultList[tempResultListTop] = cur; // The Top Element has the lowest Penalty 
				} else
				if (curcf == topPenalty) {     // More than one with same pen
					tempResultListTop++;               // Next Element in ResultList
					tempResultList[tempResultListTop] = cur;      // Save in tRL
				}
			}
		}
		// if there are more than one minimum penalty Nodes
		// choose one by Random
		if (tempResultListTop > 0) { 
			Random rnd = new Random(currRandSeed);
			int x = rnd.nextInt(tempResultListTop+1);
			return tempResultList[x];
		} else {
			return tempResultList[0];
		}
	}
	
	
	/**
	 * CHECKED:
	 * 
	 * Returns the Most Responsible Child for the given Bucket
	 * or NULL if there is no responsible child
	 *  
	 * @param q
	 * @return The Most responsible Child
	 * @throws QTreeException 
	 * @throws QTreeException 
	 */
	protected QTreeNode getMostResponsibleChildBucket(QTreeBucket q) throws QTreeException {
		if (this.children == null) return null;

		QTreeNode [] tempResultList;
		if (this.children.size() == 0) tempResultList = new QTreeNode [1]; 
		  else tempResultList = new QTreeNode [this.children.size()];
		int tempResultListTop = 0;
		double topPenalty = Double.MAX_VALUE;
		for (QTreeNode cur:this.children) {
			if (cur.isResponsibleFor(q)) {
				double curcf = cur.getDistanceBucketCenterToNodeCenter(q);				
				if (curcf < topPenalty) {
					topPenalty = curcf;
					tempResultListTop=0; // if we had another set of values before
					tempResultList[tempResultListTop] = cur; // The Top Element has the lowest Penalty 
				} else
				if (curcf == topPenalty) {     // More than one with same pen
					tempResultListTop++;               // Next Element in ResultList
					tempResultList[tempResultListTop] = cur;      // Save in tRL
				}
			}
		}
		// if there are more than one minimum penalty Nodes
		// choose one by Random
		if (tempResultListTop > 0) { 
			Random rnd = new Random(currRandSeed);
			int x = rnd.nextInt(tempResultListTop+1);
			//System.out.println("ZUFALL !!!!!!!!!!!!!!"+tempResultListTop+"!!!!!!!!!!!!!!!!!!!!!");
			return tempResultList[x];
		} else {
			return tempResultList[0];
		}
	}
	
	
	/**
	 * CHECKED:
	 * 
	 * Calculates the distance between the given Points as set of coordinates
	 * and the center of this node
	 * 
	 * @param coordinates
	 * @return The normalized Distance according to the DimSpecs
	 */
	protected double getDistanceToNodeCenter(int[] coordinates) {
		// Adapted method some parts rewritten
		
		double central = 0;
		for(int i=0;i<qtree.getNmbOfDimensions();i++){
			if (this.upperBoundaries[i] != this.lowerBoundaries[i]) {
				double c = ((double) this.upperBoundaries[i] + (double) this.lowerBoundaries[i])/2;
				double o = coordinates[i];
				//double diff = ((double) maxBounds[i] - (double) minBounds[i])/2;
				// dividing by 2 is an error --> implementation error zinn
				double diff = ((double) this.upperBoundaries[i] - (double) this.lowerBoundaries[i]);
				if (diff!=0){
					central += Math.abs((o-c) / diff);
				}
			}
		}		
		return central;
	}
	
	/**
	 * CHECKED:
	 * 
	 * Calculates the distance between the given Bucket
	 * and this Node
	 * 
	 * @param q
	 * @return The normalized Distance according to the DimSpecs
	 */
	protected double getDistanceBucketCenterToNodeCenter(QTreeBucket q) {
		
		double central = 0;
		for(int i=0;i<qtree.getNmbOfDimensions();i++){
			if (this.upperBoundaries[i] != this.lowerBoundaries[i]) {
				double c = ((double) this.upperBoundaries[i] + (double) this.lowerBoundaries[i]) / 2;
				double o = ((double) q.upperBoundaries[i] + (double) this.lowerBoundaries[i]) / 2;
				//double diff = ((double) maxBounds[i] - (double) minBounds[i])/2;
				// dividing by 2 is an error --> implementation error zinn
				double diff = ((double) this.upperBoundaries[i] - (double) this.lowerBoundaries[i]);
				central += Math.abs( (o-c) / diff);
			}
		}		
		return central;
	}
	
	
	/**
	 * CHECKED:
	 * 
	 * Enlarges the own MBB Boundarys so they also contain the coordinates
	 * of the given Bucket 
	 * 
	 * @param newBucket
	 */
	protected void enlargeBounds(QTreeNode newBucket){
		
		if (this.lowerBoundaries==null || this.upperBoundaries==null){
			this.lowerBoundaries = newBucket.lowerBoundaries.clone();	
			this.upperBoundaries = newBucket.upperBoundaries.clone();
			return;
		}
		
		for (int i=0;i<this.qtree.getNmbOfDimensions();i++){
			if (this.lowerBoundaries[i] > newBucket.lowerBoundaries[i])
				this.lowerBoundaries[i] = newBucket.lowerBoundaries[i];
			if (this.upperBoundaries[i] < newBucket.upperBoundaries[i])
				this.upperBoundaries[i] = newBucket.upperBoundaries[i];
		}
	}
	
	/**
	 * Returns this Nodes Children
	 * @return The Children of this Node 
	 */
	protected Vector<QTreeNode> getChildren(){
		return this.children;
	}
	
	
	/**
	 * Chooses these two nodes of the list that make up a merged bucket with
	 * the minimal penalty compared against all other combinations of pairs
	 * in this list
	 * @param list
	 * @return The Pair that minimizes the Penalty of the given List of QTreeNodes(Buckets)
	 */
	public Vector<QTreeNode> getPairMinimizingPenalty(Vector<QTreeNode> list, 
			QPenaltyFunction penFunction, boolean considerOnlyBucketsToMerge){
		// Generic Method for choosing a Pair of QTreeNodes or Buckets that minimizes
		// the Penalty from a given list 
		Vector<QTreeNode> BestPair = new Vector<QTreeNode>(); 

		int [] [] tempResultList;
		
		if (list.size() < 2) {
			return BestPair; 
		} else {
			tempResultList = new int [list.size()*list.size()-1][2]; // possible that all combinations have the same pen
		}
		
		// needed when trying to consider sourceIDs for penalty function 
		//int [][] candidates = new int[list.size()*list.size()-1][2];
		//double[] penalties = new double [list.size()*list.size()-1];
		
		// double penalty = calculatePenalty(list.elementAt(0), list.elementAt(1));
		double penalty = Double.MAX_VALUE;
		// represents the number of pairs that have the same highest penalty
		int tRLtop=0;
			
		// find the best combinations by testing all combinations of children
		for (int c1=0; c1<(list.size()-1); c1++){
			for (int c2=c1+1; c2<list.size(); c2++){
				Vector<QTreeNode> mergeCandidates = new Vector<QTreeNode>();
				mergeCandidates.add(list.elementAt(c1));
				mergeCandidates.add(list.elementAt(c2));
				//double pen = calculatePenalty(mergeCandidates, this.qtree.getGroupPenaltyFunction());
				double pen = calculatePenalty(mergeCandidates, penFunction);
				//pen = calculatePenalty(list.elementAt(c1), list.elementAt(c2));
				
				/*candidates[tRLtop][0] = c1;
				candidates[tRLtop][1] = c2;
				penalties[tRLtop] = pen;*/
				
				if (pen < penalty){
					tRLtop=0;
					tempResultList[tRLtop][0] = c1;
					tempResultList[tRLtop][1] = c2;
					penalty = pen;
				}
				else if (pen == penalty) { // Same penalty -> Random choose
					tRLtop++;
					// System.out.println(list.size()+" : "+tRLtop+" c1="+c1+" c2="+c2);
					tempResultList[tRLtop][0] = c1;  // Store current Child indexes
					tempResultList[tRLtop][1] = c2;
				}
			}
		}
		// best combinations have been found
		//System.err.println(Arrays.toString(penalties));
		
		
		int best1 = 0; // indexes of the best pair nodes in the input parameter list
		int best2 = 1;
		// if there is more than just one best combination --> choose randomly between them
		if (tRLtop > 0) {
			
			// consider also sourceIDs if applicable
			if (considerOnlyBucketsToMerge && 
					(!list.firstElement().getSourceIDs().isEmpty() || !list.firstElement().getSourceIDMap().isEmpty()) ){
				//for (int[] currpair: tempResultList){
				//System.out.println(tempResultList.length+", "+tRLtop);
				for (int i=0;i<=tRLtop;i++){
					int[] currpair = tempResultList[i];
					// if all sourceIDs of one bucket are already contained in the other,  
					// then merge these two
					if (!this.qtree.storeDetailedCounts()){
						if (list.elementAt(currpair[0]).getSourceIDs().containsAll(
								list.elementAt(currpair[1]).getSourceIDs()) || 
							list.elementAt(currpair[1]).getSourceIDs().containsAll(
								list.elementAt(currpair[0]).getSourceIDs())
							){
							best1 = currpair[0];
							best2 = currpair[1];
							//System.out.println(Arrays.toString(currpair));
						}
					} else {
						if (list.elementAt(currpair[0]).getSourceIDMap().keySet().containsAll(
								list.elementAt(currpair[1]).getSourceIDMap().keySet()) ||
							list.elementAt(currpair[1]).getSourceIDMap().keySet().containsAll(
								list.elementAt(currpair[0]).getSourceIDMap().keySet())	
						){
							best1 = currpair[0];
							best2 = currpair[1];							
						}
					}
				}
			} else {
				Random rnd = new Random(currRandSeed);
				int x = rnd.nextInt(tRLtop+1);
				best1 = tempResultList[x][0];
				best2 = tempResultList[x][1];
			}
		} else {
			best1 = tempResultList[0][0];
			best2 = tempResultList[0][1];
		}
		
		// return result
		BestPair.add(list.elementAt(best1));
		BestPair.add(list.elementAt(best2));
		return BestPair;
	}
	
	
	/**
	 * Reduces the fanout of this Node if it exceeds the fmax given
	 * by the QTree. Depending on the QTree Parameter mergeAtOnce
	 * the according number of childs would be grouped in one single
	 * grouping step. 
	 * @param newBucket
	 * @throws QTreeException 
	 */
	private void reduceFanout(QTreeBucket newBucket) throws QTreeException{
		
		if (this.qtree.getMergeAtOnce() == 2){
			Vector<QTreeNode> c1_c2 = 
				getPairMinimizingPenalty(this.children,this.qtree.getGroupPenaltyFunction(),false);
			
			// if newBucket is one of (c1,c2) {
			// cbase = {c1,c2} \ newBucket
			QTreeNode cbase = null;
			if (newBucket == c1_c2.get(0)) cbase = c1_c2.get(1);
			if (newBucket == c1_c2.get(1)) cbase = c1_c2.get(0);
			if (!(cbase == null)) {  // newBucket was in {c1,c2}
				
				if (!(cbase instanceof QTreeBucket)) { // if (cbase is no bucket) {
					// remove newBucket from self.childNodes
					//this.degree--;
					this.children.remove(newBucket);
					
					// cbase.addToChildren(newBucket)
					cbase.children.add(newBucket);
					//cbase.degree++;
					newBucket.setParent(cbase);
					
					if (!this.qtree.storeDetailedCounts()){
						cbase.count += newBucket.count;	
						cbase.addSourceIDs(newBucket.getSourceIDs());
					} else {
						cbase.addSourceIDsAndCount(newBucket.getSourceIDMap());
					}
					
					// cbase.adaptOwnMBB()
					cbase.enlargeBounds(newBucket);
					
					// if (#cbase.childNodes > fmax) cbase.reduceFanOut(newBucket)
					if (cbase.children.size() > this.qtree.getMaxDegree()) {
						cbase.reduceFanout(newBucket);
					}
					else {
						cbase.updatePrioQueue();
					}
					return;		
				}			
			}
			
			
			// Pushing Down not possible
			// interNode = createNewInterNode(c1,c2) with c_1 and c_2 as children
			QTreeNode newNode = new QTreeNode(this.qtree, this, c1_c2);
			if (this.qtree.storeDetailedCounts()){
				newNode.addSourceIDs(c1_c2.get(0).getSourceIDs());
				newNode.addSourceIDs(c1_c2.get(1).getSourceIDs());
			}
			
			// remove c1,c2 from self.childNodes
			this.children.remove(c1_c2.get(0));
			this.children.remove(c1_c2.get(1));
			// add interNode to self.childNodes
			this.children.add(newNode);
			
			// for each N in newNode.childNodes 
			//   N.tryToDropNode
			@SuppressWarnings("unchecked")
			Vector<QTreeNode> childlist = (Vector<QTreeNode>) newNode.children.clone(); 
			for (QTreeNode q: childlist){
				q.tryToDropNode();
			}
			newNode.updatePrioQueue();
			this.updatePrioQueue();	
			
		} else{

			Vector<QTreeNode> clist = findChildrenMinimizingPenalty(this.children,
												this.qtree.getGroupPenaltyFunction());
			
	
			// test if newBucket is contained in clist -- if not, increase basecount;
			// cbaseList afterwards contains all nodes of clist that are inner nodes
			Vector<QTreeNode> cbaselist = new Vector<QTreeNode>();
			int basecount = 0;
			for (QTreeNode q : clist) {
				if (newBucket != q) {
				  basecount++;
				  if (!(q instanceof QTreeBucket)) cbaselist.add(q);
				}
			}
			
			// if newBucket was contained in clist
			if (basecount < clist.size()) {  
				// remove all Buckets from cbase
				// and add them to the buckets to add list
				/* Not nececcary
				for (int i=0;i<cbaselist.size();i++) {
					QTreeNode q = cbaselist.get(i);
					if (q instanceof QTreeBucket) { 
						cbaselist.remove(q);					
					}
				}
				*/
				// if at least one node in clist is no bucket
				if ((cbaselist.size() > 0)) {
					// choose the one node from cbase which has the minimal
				    // penalty calculated with newBucket
					// -> doesn't work very well
					// We have to insert all other nodes too
					// if we not do this the aproximation quality is terrible
					QTreeNode cbase = null;
					/*
					double currMinPenalty = Double.MAX_VALUE;
					for (QTreeNode q : cbaselist){
						double cPen = calculatePenalty(q,newBucket);
						if (cPen < currMinPenalty) {
							currMinPenalty = cPen;
							cbase = q;
						}
					}
					*/
					int minChilds = Integer.MAX_VALUE;
					for (QTreeNode q : cbaselist){
						int cMinChilds = q.children.size();
						if (cMinChilds < minChilds) {
							minChilds = cMinChilds;
							cbase = q;
						}
					}
					// remove cbase from clist 
					clist.remove(cbase);
					
					// add the complete addList to cbase.children
					
					// remove newBucket from self.childNodes
					//this.degree--;
					//System.out.println(cbase instanceof QTreeNode);
					if (cbase.children == null) {
						// <TODO> Wieso passiert sowas ?
						this.qtree.printme();
						cbase.print("  ");
					}
					/*
					this.children.remove(newBucket);
					// cbase.addToChildren(newBucket)
					cbase.children.add(newBucket);
					//cbase.degree++;
					newBucket.setParent(cbase);
					cbase.count += newBucket.count;	
					
					// cbase.adaptOwnMBB()
					cbase.enlargeBounds(newBucket);
					*/
					for (QTreeNode q : clist){
						this.children.remove(q); // remove from own children
						cbase.children.add(q);	// add to cbase node
						q.setParent(cbase);	// adjust parent
						// Enlarge Boundaries
						cbase.enlargeBounds(q); // adjust cbase boundaries
						if (!this.qtree.storeDetailedCounts()){
							cbase.count += q.count; // adjust item count
							cbase.addSourceIDs(q.getSourceIDs());
						} else {
							cbase.addSourceIDsAndCount(q.getSourceIDMap());
						}
					}
					
					// if (#cbase.childNodes > fmax) cbase.reduceFanOut(newBucket)
					if (cbase.children.size() > this.qtree.getMaxDegree()) {
						cbase.reduceFanout(newBucket);
					}
					else {
						cbase.updatePrioQueue();
					}
					return;	
				}			
			}
			
			
			// Pushing Down not possible
			// interNode = createNewInterNode(c1,..,cn)
			QTreeNode newNode = new QTreeNode(this.qtree, this, clist);
			
			// remove c1,c2 from self.childNodes
			for (QTreeNode q : clist){
				this.children.remove(q);
				if (!this.qtree.storeDetailedCounts()){
					newNode.addSourceIDs(q.getSourceIDs());
				}
			}
			// add interNode to self.childNodes
			this.children.add(newNode);
			//this.degree--;
			
			// for each N in newNode.childNodes 
			//   N.tryToDropNode
			@SuppressWarnings("unchecked")
			Vector<QTreeNode> childlist = (Vector<QTreeNode>) newNode.children.clone(); 
			for (QTreeNode q: childlist){
				q.tryToDropNode();
			}
			newNode.updatePrioQueue();
			// In some cases it's nescessary to call tryToDropNode in this node, too
			/* <TODO> Is this true ?
			childlist = (Vector<QTreeNode>) this.children.clone();
			for (QTreeNode q: childlist){
				q.tryToDropNode();
			}
			*/
			this.tryToDropNode();
			this.updatePrioQueue();	
		}
	}
	
	/**
	 * Counts up the given position Array that is used by reduceFanOut
	 * for taking every combination of buckets from the given list into consideration 
	 * used for node grouping and bucket merging
	 * @param posArray
	 * @param n
	 * @return True if the Counter has reached its Upper Value e.q. 0000111
	 */
	private boolean incPosArray(int [] posArray,int n){
		int m = posArray.length;
		int cpos=m-1; // Start on last digit
		int mod = 0;
		boolean ovl = false;
		do {
		  posArray[cpos]++; // Increment current Position
		  int cMaxValue = n - (m - cpos);
		  mod = posArray[cpos] - cMaxValue;
		  if (mod > 0) { // Overflowhandling
			 cpos--;
			 if (cpos < 0) {
				 // Maximum Value reached
				 // reset to Max
				 for (int i=0;i<(m-1);i++) posArray[i] = n - (m-i);
				 return true; // Max Value reached
			 }
			 ovl = true;
		  }
		}while (mod > 0);
		// Propagate the new init values forward
		if (ovl){
			while (cpos < (m-1)) {
				cpos++;
				posArray[cpos] = posArray[cpos-1]+1;
			}
		}
		return false; // Max not reached
	}
	
	/**
	 * 
	 * @param f
	 * @return Returns f!
	 */
	private double faculty (int f) {
		if (f > 1) return f*faculty(f-1); else return 1;
	}
	
	/**
	 * Calculates the number of all possible combinations to set m bits to
	 * one in a bit string of length n with all other bits set to 0.  
	 * @param n
	 * @param m
	 * @return = n! / (m!(m-n)!)
	 */
	private int calcCombinations(int n, int m) {
		// calculates the number of possibilities to put m "1" Objects
		// on n places
		// = n! / (m!(m-n)!)
		return (int)(faculty(n)/(faculty(m)*faculty(n-m)));
	}
	
	/**
	 * Returns the <i>n</i> children from the given list that minimize the penalty
	 * if they would be merged together.
	 * @param list
	 * @return A List containing <i>n</i> children that minimize the Penalty
	 * @throws QTreeException
	 */
	private Vector<QTreeNode> findChildrenMinimizingPenalty(Vector<QTreeNode> list, QPenaltyFunction penFunc) throws QTreeException{
		// Generic Method for choosing a Subset of QTreeNodes or Buckets that minimize
		// the Penalty from a given list 
		Vector<QTreeNode> bestList = new Vector<QTreeNode>(); 
        int m = this.qtree.getMergeAtOnce();
        int n = list.size();
        
        // Initialize the "1" Position Array
        int [] index = new int[m];
        for (int i=0;i<m;i++) index[i]=i;
        
		int [] [] tempResultList  = null;
		// if (list.size() == 0) return BestPair;
		// TODO: destroyNode ! wenn zu wenig Kinder
		// 
		if (list.size() < this.qtree.getMergeAtOnce()) throw new QTreeException("getChildrenMinimizingPenaltyMulti: Too few Childbuckets to merge"); // oder null ?
		else tempResultList = new int [calcCombinations(n,m)][m]; // possible that all combinations have the same pen
	
		// test all combinations of children to combine to new children
		int [] best = new int [m];
		// double penalty = calculatePenalty(list.elementAt(0), list.elementAt(1));
		double penalty = Double.MAX_VALUE;
		int tRLtop=0;
			
		// TODO<MM>(DONE): Was ist wenn mehrere Children die gleiche Penalty haben ? --> Zufallsauswahl
		double pen;
		do {
			//System.out.println("ListSize "+list.size());
			//for (int i : index) System.out.println(i);
			pen = calculatePenaltyMulti(list, index, penFunc);
			if (pen < penalty){
				tRLtop=0;
				// tempResultList[tRLtop] = index.clone(); // nicht gut oder ? <TODO> Check
				for (int i=0;i<m;i++) tempResultList[tRLtop][i] = index[i]; 
				penalty = pen;
			}
			else { 
				if (pen == penalty) { // Same penalty -> Random choose
					tRLtop++;
					for (int i=0;i<m;i++) {
						if (tRLtop >= tempResultList.length) {
							throw new QTreeException("tRLtop out of bounds");
						}
						tempResultList[tRLtop][i] = index[i];  // Store current Child indexes
					}
				}
			}
		} while (!incPosArray(index,n));
		// best combination(s) found
		
		if (tRLtop > 0) { 
			Random rnd = new Random(currRandSeed);
			int x = rnd.nextInt(tRLtop+1);
			//System.out.println("ZUFALL !!!!!!!!!!!!!!"+tempResultList+"!!!!!!!!!!!!!!!!!!!!!");
			best = tempResultList[x];
		} else {
			best = tempResultList[0];
		}
		
		// System.out.println("Best Pair is ("+best1+","+best2+")");
		for (int i=0;i<best.length;i++){
			bestList.add(list.elementAt(best[i]));
		}
		return bestList;
	}
	
	/**
	 * Updates this node's entry in the priority queue or removes
	 * this node out of it if there are too few children
	 * CHECKED: 
	 * @throws QTreeException 
	 *
	 */
	protected void updatePrioQueue() throws QTreeException {
		// if this is a bucket -- return because buckets do not have children
		if (this instanceof QTreeBucket) {
			return;
		}
		
		// identify all children that are buckets
		Vector<QTreeNode> bucketChildren = new Vector<QTreeNode>();
		for (QTreeNode q: this.children){
			if (q instanceof QTreeBucket) bucketChildren.add(q);
		}
		
		// get those (two) children with the minimum merge penalty
		// go on only if there are at least 2 (mergeAtOnce) children that are buckets
		if (bucketChildren.size() < this.qtree.getMergeAtOnce()) { 
			this.qtree.getPrioQueue().remove(this);
			this.nextToMerge = null;
			this.nextToMergePen = Double.MAX_VALUE;
			return;
		}
		
		if (this.qtree.getMergeAtOnce()==2){
			this.nextToMerge = getPairMinimizingPenalty(bucketChildren,this.qtree.getMergePenaltyFunction(),true);
		} else{
			this.nextToMerge = this.findChildrenMinimizingPenalty(bucketChildren,
					this.qtree.getMergePenaltyFunction());
		}
		
		//this.nextToMergePen = calculatePenalty(nextToMerge.elementAt(0), nextToMerge.elementAt(1)); 
		this.nextToMergePen = this.calculatePenalty(this.nextToMerge,this.qtree.getMergePenaltyFunction());

		// Update des Eintrags, falls nicht existiert passiert auch nix
		this.qtree.getPrioQueue().remove(this);
		this.qtree.getPrioQueue().add(this);
	}
	
	/**
	 * Returns the number of children
	 * 
	 * @return The Number of Children
	 */
	protected int degree(){
		if (this.children == null) {
			return 0;
		} else {
			return this.children.size();
		}
	}
	
	
	/**
	 * CHECKED
	 * 
	 * Tries to remove Inner Nodes if Buckets have been merged and
	 * the result bucket could be inserted into the parent node without
	 * violating the fmax constrain
	 *  
	 * @return True if a Node was removed
	 * @throws QTreeException 
	 */
	protected boolean tryToDropNode() throws QTreeException{
		// root note can't be dropped
		if (this == this.qtree.getRoot()){
			return false;
		}
		
		// buckets cannot be dropped either
		if (this instanceof QTreeBucket){
			return false;
		}
		
		if ((parent.degree() + this.degree() - 1) > this.qtree.getMaxDegree() ){
			return false;
		}
		
		this.parent.children.remove(this);
		
		// parent.addToChildren(self.children)
		for (QTreeNode q: this.children) {
			parent.children.add(q);
			q.setParent(this.parent);
		}
		this.children.removeAllElements();
		
		this.qtree.getPrioQueue().remove(this);
		
		this.parent.updatePrioQueue();
		// this.parent.tryToDropNode();		// evtl. rekursiv weiter
		return true;
	}
	
	
	/**
	 * 
	 * @param nodes A List of Nodes that should be grouped/merged
	 * @return The Resulting Penalty if all Nodes on the List are merged together
	 */
	private double calculatePenalty(Vector <QTreeNode> nodes, QPenaltyFunction penFunc) {
		// create shallow clone copy from first node in list
		QTreeNode nodeUnited = nodes.get(0).newShallowCopyForBounds();
		// enlarge boundaries such that the resulting node encompasses the MBBs of all nodes in the input list <emph>nodes</emph>
		for (int i=1;i < nodes.size();i++) {
			nodeUnited.enlargeBounds(nodes.get(i)); 
		}
		// compute penalty value based on the "merged" node
	    return penFunc.calculatePenalty(this.qtree.getDimSpecMin(), this.qtree.getDimSpecMax(), 
	    						 nodeUnited);
	}
	
	/**
	 * 
	 * @param nodes A List of Nodes that should be grouped/merged
	 * @param indexes A List of indices which nodes of the list
	 *                should be merged(not all) 
	 * @return The Penalty regarding to the Current Penalty Function
	 */
	private double calculatePenaltyMulti(Vector <QTreeNode> nodes,int [] indexes,
										 QPenaltyFunction penFunc){
		
		Vector <QTreeNode> nList = new Vector<QTreeNode>();
		for (int i : indexes) {
            // Check whether the Max Indexed Value from Index Array 
			// dont exceed the Size of the NodeList given
			assert (i <= nodes.size()) : "QTreeNode.caclulatePenalty: Error in IndexArray generating Algorithm";
			nList.add(nodes.get(i));
		}
		return calculatePenalty(nList,penFunc);
	}
	
	
	/**
	 * CHECKED:
	 * 
	 * Shallow copy of this node
	 * 
	 * @return A Copy of this Node 
	 */
	protected QTreeNode newShallowCopyForBounds(){
		QTreeNode newNode = new QTreeNode(this.qtree, this.parent, 
				this.lowerBoundaries.clone(), this.upperBoundaries.clone(), this.count, 
				this.getPeerID(), this.getNeighborID(), 
				this.getLocalBucketID(), this.getSourceIDs());
		newNode.children = this.children;

		return newNode;
	}	
	
		
	/**
	 * Set the own parent to be the given node 
	 * @param node
	 */
	protected void setParent(QTreeNode node){
		this.parent = node;
	}
	
	
	/**
	 * CHECKED:
	 * 
	 * Reduces the count of the Buckets in this Node by merging
	 * the both with the Minimum Penalty value
	 * The both Childs(Buckets) are in nextToMerge and the
	 * according penalty value is in nextToMergePen
	 * @throws QTreeException 
	 * @throws Exception 
	 *
	 */
	protected void reduceBuckets() throws QTreeException{
		
		// both (=all) children are buckets, if not there wouldn't have been an entry in the priority queue 
		if (this.degree() == this.qtree.getMergeAtOnce()) {
			// this.convert1NodeWith2ChildrenTo1Bucket();
			this.convertToBucket(); 
			if (this == this.qtree.getRoot()) {
				this.updatePrioQueue();
			} else {
				this.parent.updatePrioQueue();
			}
			return;
		}
		
		//QTreeNode newNode = new QTreeNode(this.qtree, this, nextToMerge.get(0), nextToMerge.get(1));
		// create an inner node with the two nodes referenced by nextToMerge as children
        QTreeNode newNode = new QTreeNode(this.qtree, this, this.nextToMerge);
		this.children.add(newNode);
		//newNode.convert1NodeWith2ChildrenTo1Bucket(); // decreases the number of buckets counter 
		newNode.convertToBucket();
		
		// Replace c1 and c2 by Internode in self.children
		//this.children.remove(nextToMerge.get(0));
		//this.children.remove(nextToMerge.get(1));
		for (int i=0;i < this.nextToMerge.size();i++){
			QTreeNode q = this.nextToMerge.get(i);
			this.children.remove(q);
		}
		
		this.nextToMerge = null; // muss nicht unbedingt, da in updatePQ neu gesetzt
		if (!this.tryToDropNode()){
			this.updatePrioQueue();
		}
	}
	
	
	/**
	 * CHECKED
	 * 
	 * Converts a QTreeNode into a Single Bucket
	 * 
	 * Limitation: works only for merging when the node has only two buckets
	 */
	protected void convertToBucket(){
		// assert that all children are buckets
		for (QTreeNode currNode: this.children){
			assert (currNode instanceof QTreeBucket);
		}
	
		// Find the Minimum Local Bucket ID
        long minLocalBucketID = Long.MAX_VALUE;
		for (QTreeNode b : this.children){
			if (b.getLocalBucketID() < minLocalBucketID) {
				minLocalBucketID = b.getLocalBucketID();
			}
		}
		
		// Create a new Bucket
		// and use the Minimum Local Id found above
	    //QTreeBucket newBucket = new QTreeBucket(this.qtree, children.elementAt(0).minBounds, this.parent);
		//QTreeBucket newBucket = new QTreeBucket(this.qtree, children.elementAt(0).minBounds, children.elementAt(0).maxBounds, children.elementAt(0).count, this.parent);
		
		//QTreeBucket newBucket = new QTreeBucket(this.qtree, this.parent, this.lowerBoundaries, this.upperBoundaries, 
		//		this.children.get(0).count, this.qtree.getPeerID(), this.qtree.getNeighborID(), minLocalBucketID);
		
		QTreeBucket newBucket = null;
		// initialize the new bucket with the boundaries and count of its first child
		if (!this.qtree.storeDetailedCounts()){
			newBucket = new QTreeBucket(this.qtree, this.parent, this.children.get(0).lowerBoundaries, this.children.get(0).upperBoundaries, 
				this.children.get(0).count, this.qtree.getPeerID(), this.qtree.getNeighborID(), minLocalBucketID, this.children.get(0).getSourceIDs());
		} else {
			newBucket = new QTreeBucket(this.qtree, this.parent, this.children.get(0).lowerBoundaries, this.children.get(0).upperBoundaries, 
				this.qtree.getPeerID(), this.qtree.getNeighborID(), minLocalBucketID, this.children.get(0).getSourceIDMap());
		}
		
		if (Parameters.storeDataPointsInBuckets) {
			newBucket.addPoints(this.children.get(0).getContainedPoints());
		}
		//newBucket.localBucketID = findUniqueBucketID(this.children);
		//newBucket.setLocalBucketID(findUniqueLocalBucketID(this.children));
		//newBucket.setGlobalBucketID(findUniqueGlobalBucketID(this.children));

		// Enlarge Boundaries & Increase Count for all other children
		for (int i=1;i < this.children.size();i++){
			newBucket.enlargeBounds(this.children.elementAt(i));
			if (!this.qtree.storeDetailedCounts()){
				newBucket.count += children.elementAt(i).count;
				// merge sourceIDs
				newBucket.addSourceIDs(this.children.elementAt(i).getSourceIDs());
			} else {
				newBucket.addSourceIDsAndCount(this.children.elementAt(i).getSourceIDMap());
			}
			
			// Only for Evaluation: Store contained Points also
			if (Parameters.storeDataPointsInBuckets) {
				newBucket.addPoints(this.children.get(i).getContainedPoints());
			}
		}
		
		// ClearFromPQ -> schon durch PriorityQueue.pop erledigt
		
		// decrement bucket counter
		for (int i=0;i<this.qtree.getMergeAtOnce()-1;i++) {
			this.qtree.decCurrBucketCount();
		}
		
		if (this == this.qtree.getRoot()) {		
			this.children.removeAllElements();	// delete references to children -- so that they can be deleted by the garbage collector
			this.children.add(newBucket);
			newBucket.setParent(this);
		} else {
			this.parent.children.remove(this);       // remove old Entry of this QTreeNode
			this.parent.children.add(newBucket);     // with new created Bucket
			this.children.removeAllElements();
		}
	}
		
	/**
	 * CHECKED:
	 * 
	 * Returns a List containing all Buckets of this Node and its childs
	 * @param BucketList
	 */
	protected void getAllBuckets(Vector<QTreeBucket> BucketList){
		for (QTreeNode child : this.children) {
			if (child instanceof QTreeBucket) BucketList.add((QTreeBucket) child);
			  else child.getAllBuckets(BucketList);
		}
	}
	
	
	/**
	 * @author hose
	 * 
	 * This method inserts all QTreeBuckets into the HashMap that has been given as 
	 * input parameter.
	 * @param bucketList The List of Buckets that should be inserted
	 *
	 */
	protected void getAllBucketsInHashMapGlobalID(HashMap<String, Bucket> bucketList){
		
		for (QTreeNode child : this.children) {
			
			if (child instanceof QTreeBucket){ 
				bucketList.put(new String(((QTreeBucket)child).getGlobalBucketID()), child);
			} else {
				child.getAllBucketsInHashMapGlobalID(bucketList);
			}
			
		}
	}
	
	
	/**
	 * Method that returns a flat clone of this object. 
	 * Note that referenced objects are <b>NOT</b> cloned!
	 * @return Flat! Clone of this Node
	 */
	@SuppressWarnings("unchecked")
	public QTreeNode clone(){		
		// flat copy
		QTreeNode clone = (QTreeNode) super.clone();
		
		// Clone simple Structures
		clone.lowerBoundaries = this.lowerBoundaries.clone();
		clone.upperBoundaries = this.upperBoundaries.clone();
		
		return clone;
	}
	
	/**
	 * Clones the whole QTree Structure recursively
	 * 
	 * @param newQTree The New QTree used for this.qtree
	 * @param newParent The New Parent used for this.parent
	 * @param assocTable The Association Table build to clone the PriorityQueue
	 */
	@SuppressWarnings("unchecked")
	protected void cloneStructure(QTree newQTree,QTreeNode newParent,HashMap assocTable){
		this.qtree = newQTree;
		this.parent = newParent;
		// Replaces all inner Nodes from here on ..
		// Assume all "Simple" Structures are already cloned(by QTreeNode.clone)
		Vector<QTreeNode> newChildList = new Vector<QTreeNode>(); // This will be the new childList
		Vector<QTreeNode> newNextToMerge = new Vector<QTreeNode>(); // This will be the new nextToMerge
		for (QTreeNode child : this.children){
			// Make a flat Copy including the cloned structures w/o children and nextToMerge
			QTreeNode clonedChild = child.clone();
			newChildList.add(clonedChild);
			// If the current child is in nextToMerge the cloneChild
			// has to be in newNextToMerge , nextToMergePen is already cloned
			if (this.nextToMerge != null) {
				if (this.nextToMerge.contains(child)) newNextToMerge.add(clonedChild);
			}
			// Remember the Assocciation
			assocTable.put(child, clonedChild);
			// Repeat recursively
			clonedChild.cloneStructure(newQTree, this, assocTable);
		}
		// Replace the old Structure Lists withn the new ones
		this.children = newChildList;
		this.nextToMerge = newNextToMerge;
	}
	
	/**
	 * Print current boundaries to the Standard OutPut
	 */
	public void printbounds(){
		QTreeNode a = this;
		for (int i=0;i < this.qtree.getNmbOfDimensions()-1;i++) {
			System.out.print(a.lowerBoundaries[i]+",");
		}
		System.out.print(a.lowerBoundaries[this.qtree.getNmbOfDimensions()-1]+") - (");
		for (int i=0;i < this.qtree.getNmbOfDimensions()-1;i++) {
			System.out.print(a.upperBoundaries[i]+",");
		}
		System.out.println(a.upperBoundaries[this.qtree.getNmbOfDimensions()-1]+") Count: "+a.count);	
	}
	
	/**
	 * Returns the current boundaries into a String
	 * @return A String describing the current Boundaries
	 */
	public String getBoundsString(){
		String bs = "L[";
		QTreeNode a = this;
		if (a.lowerBoundaries!=null){
			for (int i=0;i < this.qtree.getNmbOfDimensions()-1;i++) {
				bs += a.lowerBoundaries[i]+",";
			}
			bs += a.lowerBoundaries[this.qtree.getNmbOfDimensions()-1]+"] - H[";
			for (int i=0;i < this.qtree.getNmbOfDimensions()-1;i++) {
				bs += a.upperBoundaries[i]+",";
			}
			bs += a.upperBoundaries[this.qtree.getNmbOfDimensions()-1]+"] ";
		}
		//bs += ", count= "+a.count;		
		if (!a.getSourceIDs().isEmpty()) {
			bs += ", count= "+a.count;
			bs += ", sources ("+a.getSourceIDs().size()+")";
			//bs += "= "+a.getSourceIDs().toString();
		} 
		if (!a.getSourceIDMap().isEmpty()){
			count = getTotalCount();
			bs += ", count= "+count;
			bs += ", sourceIDMap ("+a.getSourceIDMap().size()+")";
			//bs += "= "+a.getSourceIDMap().toString();
		}
		bs += "\n";
		return bs;
	}
	
	public double getTotalCount(){
		double count = 0.0;
		for (Entry<String,Double> ent: this.getSourceIDMap().entrySet()){
			count += ent.getValue();
		}
		return count;
	}
	
	/**
	 * Prints a Recursive Debug Output from this and all following Nodes
	 * @param space The Space Chars that are used representing a higher depth
	 */
	public void print(String space) {
		if (this == this.qtree.getRoot()) System.out.println("ROOT");
		if ((this.children.size() == 0) || (this instanceof QTreeBucket)) return;
		for (QTreeNode a : this.children){
			System.out.print(space);
			if (a instanceof QTreeBucket) {
				System.out.print("Bucket: (");
				a.printbounds();
				//+a.minBounds[1]+"-"+a.maxBounds[0]+","+a.maxBounds[1]+" - Count: "+a.count);
			}
			else { 
				System.out.print("Node  : (");
				a.printbounds();
				a.print(space+"  ");
			}
		}
	}
	
	
	/**
	 * Returns a String which represents the Structure of the Tree 
	 * @param space The Space that are used representing a higher depth
	 * @return The Structure String
	 */
	public String getStructureString(String space){
		String ss="";
		if (this == this.qtree.getRoot()) ss += "  ROOT "+this.getBoundsString();
		if ((this.children.size() == 0) || (this instanceof QTreeBucket)) return "";
		for (QTreeNode a : this.children){
			ss += space;
			if (a instanceof QTreeBucket) {
				//ss += "Bucket: ";
				ss += "Bucket(LID "+((QTreeBucket)a).getLocalBucketID()+", GID "+((QTreeBucket)a).getGlobalBucketID()+"): ";
				ss += a.getBoundsString();
				//+a.minBounds[1]+"-"+a.maxBounds[0]+","+a.maxBounds[1]+" - Count: "+a.count);
			}
			else { 
				ss += "Node  : ";
				ss += a.getBoundsString();
				ss += a.getStructureString(space+"  ");
			}
		}		
		return ss;
	}
	
	/**
	 * @deprecated
	 * @return
	 */
	public QTreeNode getParent(){
		return this.parent;
	}
	

	/**
	 * This is the recursive Method for checking whether the Tree has Data in
	 * some Region 
	 * 
	 * TODO: do not run through all nodes but only consider those whose MBBs support the conditions
	 * 
	 * @param attrNames Array containging the Names for the interesting Dimensions
	 * @param ops Operations that should be used for Comparison
	 * @param attrvals The comparing Values
	 * @return False if there is defintivly no Data else True
	 * @throws QTreeException 
	 */
	protected boolean checkDataAvail(String [] attrNames,
			                      int [] ops,
			                      double [] attrvals) throws QTreeException{
		for (QTreeNode q:this.children){
			if (q.checkDataAvail(attrNames,ops,attrvals)) return true;
		}
		return false;
	}
	
	/**
	 * Returns the Number of the Current Buckets
	 * @return Current Bucket Count
	 * 
	 * @author hose
	 */
	public int getNmbOfCurrentBuckets(){
		return this.nmbOfCurrentBuckets;
	}
	
	/**
	 * ATTENTION: when using this method the call method has to take care of updating the priority queue !!!
	 * 
	 * 
	 * @param newChild
	 * 
	 * @author hose
	 */
	public void addChild(QTreeNode newChild){
		this.children.add(newChild);
	}
		
	/**
	 * Updates this Nodes count by the delta updCount(positive or negative)
	 * Thereby the parent Node would be called recursively
	 * 
	 * @param updCount
	 */
	protected void updateCountBottomUp(double updCount){
		this.count += updCount;
		if (this.parent != null) this.parent.updateCountBottomUp(updCount);
	}

	
	protected void updateCountBottomUp(double updCount, String source){
		
		if (!this.qtree.storeDetailedCounts()){
			
			this.count += updCount;
			
		} else {
			// added by MKa to have the global count updated as well
			this.count += 1.0;
			
			Double count = this.sourceIDCountMap.get(source);
			if (count == null){
				this.sourceIDCountMap.put(source, new Double(1.0));
			} else {
				this.sourceIDCountMap.put(source, new Double(count.doubleValue()+1));
			}
		}
		
		if (this.parent != null) this.parent.updateCountBottomUp(updCount, source);
	}
	
	
	/**
	 * Updates this Nodes count by the delta updCount(positive or negative) <br>
	 * Additionally this Nodes boundaries are adjusted <br>
	 * Regulary called when Buckets are deleted completely <br>
	 * Calls its parent Node recursively
	 * 
	 * @param updCount
	 */
	protected void updateCountAndBoundsBottomUp(double updCount){
		// Adjust Count
		this.count += updCount;
		// Adjust Bounds
		if (this.children.size() > 0) {
			QTreeNode dummy = this.children.firstElement().newShallowCopyForBounds();
			for (QTreeNode child : this.children){
				dummy.enlargeBounds(child);
			}
			this.lowerBoundaries = dummy.lowerBoundaries;
			this.upperBoundaries = dummy.upperBoundaries;
		}
		if (this.parent != null) this.parent.updateCountAndBoundsBottomUp(updCount);
	}
	
	/* (non-Javadoc)
	 * @see smurfpdms.index.Bucket#getAttributeNames()
	 */
	@Override
	public Vector<String> getAttributeNames() {
		return this.qtree.getAttributeNames();
	}


	/* (non-Javadoc)
	 * @see smurfpdms.index.Bucket#updateCount(double)
	 */
	@Override
	public void updateCount(double amount) {
		this.updateCountBottomUp(amount);
	}
	
	/* (non-Javadoc)
	 * @see smurfpdms.index.Bucket#updateCount(double)
	 */
	@Override
	public void updateCount(double amount, String sourceID) {
		if (this.qtree.storeDetailedCounts()){
			this.updateCountBottomUp(amount, sourceID);
		} else {
			this.updateCountBottomUp(amount);
		}
	}
	
	
	/**
	 * method that builds a subtree of DefaultMutableTreeNodes with 
	 * the given <em>parentNode</em> as root node. This is used to build a JTree for displaying
	 * index information for the user.
	 * 
	 * @param parentNode the root node of the whole subtree
	 */
	@SuppressWarnings("nls")
	public void buildJSubTree(DefaultMutableTreeNode parentNode) {

		String text = "";

		if (this == this.qtree.getRoot()) {
			// if this is the root node
			text += "QTreeRootNode " + this.getBoundsString();

		} else if (this instanceof QTreeBucket) {
			// if this is a leaf node, i.e. bucket
			text += "QTreeBucket ID=[" + ((QTreeBucket) this).getLocalBucketID() + ","
					+ ((QTreeBucket) this).getGlobalBucketID() + "]" + ", space="
					+ StringHelper.arrayToString(((QTreeBucket) this).getLowerBoundaries()) + "-"
					+ StringHelper.arrayToString(((QTreeBucket) this).getUpperBoundaries())
					+ ", count= " + ((QTreeBucket) this).getCount();

		} else {
			// if this is an inner node
			text += "QTreeNode " + this.getBoundsString();
		}

		DefaultMutableTreeNode node = new DefaultMutableTreeNode(text);
		parentNode.add(node);

		for (QTreeNode child : this.children) {
			child.buildJSubTree(node);
		}
	}
	
	
	/**
	 * 
	 * @param updBucket
	 * @param currentBucket
	 * @return The Difference Penalty
	 */
	private double calcDiffPen(QTreeBucket updBucket, QTreeBucket currentBucket){
		Vector<QTreeNode> penCalcList = new Vector<QTreeNode>();
		penCalcList.add(updBucket);
		double buckPen = calculatePenalty(penCalcList, this.qtree.getMergePenaltyFunction());			
		penCalcList.add(currentBucket);
		double mergePen = calculatePenalty(penCalcList, this.qtree.getMergePenaltyFunction());
		return (mergePen - buckPen);
	}
	
	/**
	 * Finds the appropriate Local Bucket that matches the given <i>updateBucket</i>
	 * best ... Thereby the resulting Penalty and the Count of the Bucket are taken
	 * into consideration
	 * 
	 * @param bucket The UpdateBucket as a QTreeBucket
	 */
	protected void removeBucketwithHeuristics(QTreeBucket bucket)throws Exception{
		System.out.println("removeBucketwithHeuristics");
		Vector<QTreeBucket> buckList = this.qtree.getAllBuckets();
		// Create a Dummy QTreeBucket for Penalty Calculation
		QTreeBucket updBucket = bucket;
		
		// Calculate the Difference Penalty(for enlarging the matching bucket)
		// and sort the buckets ordered by this penalty
		QTreeBucket [] resList = new QTreeBucket[buckList.size()];
		double [] penList = new double[buckList.size()];
		
		penList[0] = calcDiffPen(updBucket,buckList.get(0));
		resList[0] = buckList.get(0);
		for (int i=1;i<buckList.size();i++){
			double cpen = calcDiffPen(updBucket,buckList.get(i));
			int j=i-1;
			while ((j>=0) && (penList[j]>cpen)) {
				// shift all bigger elements to the right
				penList[j+1] = penList[j];
				resList[j+1] = resList[j];
				j--;
			};
			penList[j+1] = cpen;
			resList[j+1] = buckList.get(i);
		}
		
		// find a bucket that has enough items to be reduced
		// Note: This could result in a much worse approximation quality !! 
		int i=0; boolean found=false;
		while ((i<resList.length) && (!found)){
			if (resList[i].count > (bucket.getCount()+0.5)) found=true;
			else i++;
		}
		if (found) {
			// Take the Bucket with a matching count
			resList[i].enlargeBounds(bucket);
			resList[i].count -= bucket.count;
			resList[i].parent.updateCountAndBoundsBottomUp(-bucket.count);
			resList[i].parent.updatePrioQueue();
		} else {
			// If this also fails, we have to split the reduction to more than
			// one Bucket and eventually remove Buckets
			System.err.println("updateBucket: Seltener Fall eingetreten");
			System.err.println("updatecount muss aufgeteilt werden !");
			System.err.println("Buckets werden evtl. entfernt");
			// Heuristic:
			System.err.println("resList.length = "+resList.length);
			int j=0;
			double remainCount = bucket.getCount();
			while ((j<resList.length) && (remainCount > 0)){
				double reduceCount = resList[j].count;
				if (reduceCount > remainCount) {
					resList[j].count -= remainCount;
					resList[j].enlargeBounds(bucket);
					resList[j].parent.updateCountAndBoundsBottomUp(-remainCount);
					resList[j].updatePrioQueue();
				} else {
					// This Bucket has to be deleted
					resList[j].enlargeBounds(bucket);
					QTreeBucket dummy = new QTreeBucket(null,null,
													    bucket.lowerBoundaries,
													    bucket.upperBoundaries,
													    resList[j].count,
													    "0","0",0, new HashSet<String>());
					resList[j].removeDataItem(dummy);
				}
				remainCount -= reduceCount;
			}
		}
	}
	
} // END