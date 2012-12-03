/**
 * 
 */
package de.ilmenau.datasum.index.qtree;


import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import de.ilmenau.datasum.exception.QTreeException;
import de.ilmenau.datasum.index.BucketBasedCompoundRoutingIndex;
import de.ilmenau.datasum.index.Index;
import de.ilmenau.datasum.index.LocalIndex;
import de.ilmenau.datasum.peer.Neighbor;


/**
 * @author Matz
 *  This class represents a Routing Index based on the QTree
 */
@SuppressWarnings("serial")
public class QRoutingIndex extends BucketBasedCompoundRoutingIndex {
	
	
	/** Maximum Fanout of a Node */
	private int maxFanout;	 

	/** Maximum Buckets this Tree could have */
	private int maxBuckets;

	/** This Hash-Map is used to acces each neighbours Data separately */
	private HashMap<Neighbor,QTree> NBQTreeList;
	
	/** Holds the number of current Buckets */
	private int currBuckets;
	
	/** minimum values for each dimension for normalizing */
	private int[] dimSpecMin;
	/** maximum values for each dimension for normalizing */
	private int[] dimSpecMax;
	
	/** attribute names of the dimensions which the QTree is defined on */
	private Vector<String> indexOnDimensions;
	
	// 2 Methods of Bucket Reduction(only usable at createIndex)
	// ALL - Build Neighbour QTrees completely first
	//       Reduce in One Big Iteration afterwards
	// STEP - Reduce stepwise after each Bucket Insertion
	// Choose one of them
	//private static final String BUCKET_REDUCTION="ALL";
	private static final String BUCKET_REDUCTION="STEP";
		
	/**
	 * constructor
	 */
	public QRoutingIndex(){
		super(IndexType.QTree);
	}
	
	/**
	 * 
	 * @param indexOnDimensions
	 * @param dimSpecMin
	 * @param dimSpecMax
	 * @param maxFanout
	 * @param maxNmbOfBuckets
	 */
	public QRoutingIndex(Vector<String> indexOnDimensions, 
						 int[] dimSpecMin,
						 int[] dimSpecMax,
						 int maxFanout,
						 int maxNmbOfBuckets){	
		super(IndexType.QTree);	
		
		this.dimSpecMin = dimSpecMin.clone();
		this.dimSpecMax = dimSpecMax.clone();
		this.maxFanout = maxFanout;
		this.maxBuckets = maxNmbOfBuckets;
		this.indexOnDimensions = indexOnDimensions;
		this.NBQTreeList = new HashMap<Neighbor,QTree>();
		this.currBuckets = 0;
	}
	
	/**
	 * Checks whether the given Local Index Object is Part of this Routing Index
	 * 
	 * @param qt
	 * @return True if the given Object <i>qt</i>is Part of this Routing Index
	 */
	public boolean checkIfContainsLocalIndex(QTree qt){
		return this.NBQTreeList.containsValue(qt);
	}
	
	/**
	 * constructs the complete QRouting Index for one Peer Object
	 * each Neighbor gets its own QTree, and all these were put
	 * together into one HashMap
	 * 
	 * @param thisPeersNeighborsIndexes list of local indexes that are to be combined into just one index
	 * @param indexOnDimensions list of attribute names that are indexed with the indexes
	 * @param dimSpecMin 
	 * @param dimSpecMax
	 * @param maxFanout
	 * @param maxNmbOfBuckets
	 * @return TODO
	 * @throws Exception 
	 */
	public static QRoutingIndex createIndex(HashMap <Neighbor,Vector<Index>>thisPeersNeighborsIndexes,
											Vector<String> indexOnDimensions, 
	    									int[] dimSpecMin,
	    									int[] dimSpecMax,
	    									int maxFanout,
	    									int maxNmbOfBuckets,
	    									boolean storeDetailedCounts) throws Exception{
		
		//System.out.println("creating QRI for neighbors: "+thisPeersNeighborsIndexes.keySet());
		
		// Create new QRoutingIndex Object
		QRoutingIndex QRI = new QRoutingIndex(indexOnDimensions,
				 							  dimSpecMin, 
				 							  dimSpecMax,
				 							  maxFanout,
				 							  maxNmbOfBuckets);
		
		for (Neighbor currNB: thisPeersNeighborsIndexes.keySet()){		
			// Neuer QTree fuer jeden Nachbarn
			//QTree NQTree = (QTree) currNB.createEmptyLocalIndex();
			QTree NQTree = new QTree(indexOnDimensions.size(), 
					maxFanout, maxNmbOfBuckets, dimSpecMin.clone(), 
					dimSpecMax.clone(), currNB.getPeerID(), 
					currNB.getNeighborID(), storeDetailedCounts);
		
			NQTree.setAttributeNames(indexOnDimensions);			
			NQTree.setParentQRoutingIndex(QRI);
			QRI.NBQTreeList.put(currNB, NQTree);

			Vector<Index> indexList = thisPeersNeighborsIndexes.get(currNB);
			for (Index currLRI: indexList ) { // Add each Index seperatly			
				QTree currNewQTree = (QTree) currLRI;
				Vector<QTreeBucket> bucklist = currNewQTree.getAllBuckets();
				for (QTreeBucket currBucket : bucklist) {
					NQTree.insertDataItem(currBucket,true,true);	// cares for a deep cloning
					if (BUCKET_REDUCTION.equals("STEP")) QRI.reduceBuckets(false);
				}
			}
			
			// be sure that the parent name is set!
			/*if (NQTree.getParentNodeName() == null) {
				NQTree.setParentNodeName(
					((QTree) currNB.getCorrespondingPeerObject().getLRI()).getParentNodeName()
				);
			}*/
		}
		
		if (BUCKET_REDUCTION.equals("ALL")) QRI.reduceBuckets(true);
		return QRI;
	 }	
	
	/**
	 * TODO: implement this method
	 * 
     * abstract method for determining if this index object is empty or not 
     * 
     * @return true if index has no data
     * 
     */
    public boolean isEmpty(){
    	return this.NBQTreeList.isEmpty();
    }
    
    /**
     * Inserts the given Local Index from Neighbor into the RoutingIndex.
     *   
     * @param inputIndex
     */
    public void addIndexData(Neighbor neighbor, LocalIndex inputIndex){
    	QTree currNewQTree = (QTree) inputIndex;
    	
    	if (this.NBQTreeList == null) {
    		this.NBQTreeList = new HashMap<Neighbor, QTree>();
    	}
    	
    	QTree NQTree = this.NBQTreeList.get(neighbor);
    	if (NQTree != null) {
			//NQTree = (QTree) neighbor.createEmptyLocalIndex();
			NQTree.setAttributeNames(currNewQTree.getAttributeNames());			
			NQTree.setParentQRoutingIndex(this);
			this.NBQTreeList.put(neighbor, NQTree);
    	}
    	
    	// As in MultiDimHistograms always set to the ParentNodeName of the inputIndex ...
		NQTree.setParentNodeName(currNewQTree.getParentNodeName());
		
    	Vector<QTreeBucket> bucklist = currNewQTree.getAllBuckets();
		for (QTreeBucket currBucket : bucklist) {
			//System.out.println("Insert Bucket "+currBucket.getBoundsString());
			//System.out.println("Insert Bucket "+currBucket.globalBucketID);
			//QRI.currBuckets++; --> leads to error when bucket is inserted into an existing one
			
			// return value of insertDataObject(currBucket) could be negative or positive
			try {
				NQTree.insertDataItem(currBucket,true,true);
				reduceBuckets(false);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	// cares for a deep cloning
			 // We allways have to reduce
		}    	
    }
    

	/**
	* method for getting the type value of an Index object.
	*/
    public IndexType getType(){
        return type;
    }
    
    
    public String getStateString(){
    	return "";
    }
    
	/**
	* abstract method for getting the state string of the Index object.
	 * @param n Neighbor The State String should be returned
	 * @return The Statestring of Neighbor <b>n</b>
	*/
    public String getStateStringForNeighbor(Neighbor n){
    	String returnString = "";
    	returnString += "maxBuckets CRI: "+this.maxBuckets +"\n";
    	returnString += "currBuckets: "+this.NBQTreeList.get(n).getCurrBuckets()+"\n";
    	if (NBQTreeList.get(n).getAllRecentUpdates() == null){
    		returnString += "Recent Updates: "+NBQTreeList.get(n).getAllRecentUpdates()+"\n";
    	} else {
    		returnString += "Recent Updates "+NBQTreeList.get(n).getAllRecentUpdates().size()+": "+NBQTreeList.get(n).getAllRecentUpdates()+"\n";
    	}
    	returnString += this.NBQTreeList.get(n).getRoot().getStructureString("    "); 
    	return returnString;
    }
	
    /**
     * CHECKED: 
     * 
     * Reduces the global Bucket Count
     * @param finalCheck indicates whether this method is called after all buckets have been inserted or not
     * @throws QTreeException 
     * 
     * TODO<MM>: checken, ob es auch wirklich niemals sein kann, dass hier versucht wird, 2 rootNodes von verschiedenen QTrees zu mergen --> sollte eigentlich nie passieren, aber checken kann man das trotzdem vorher noch mal
     *
     */
    public void reduceBuckets(boolean finalCheck) throws QTreeException{
    	// TODO<MM>: checken, ob der currBucket-Zaehler des QRoutingIndex auch wirklich IMMER den aktuellen Wert (Summe aller seiner Kinder) entspricht !!!
    	
    	assert this.currBuckets == this.getGlobalBucketCount() : 
    		   "currBuckCount="+this.currBuckets+" != Sum of QTree Counts = "+this.getGlobalBucketCount();
    	
    	if ( !(finalCheck || BUCKET_REDUCTION.equals("STEP")) ){
    		return;
    	}
    	    	
    	// TODO<MM>: checken, ob der currBucket-Zaehler des QRoutingIndex auch wirklich IMMER den aktuellen Wert (Summe alle seiner Kinder) entspricht !!! --> jetzt anscheinend ja --> trotzdem noch mal checken
    	// TODO<MM>: hier stimmt irgendwas nicht --> manchmal nicht diejenigen buckets gemerged, die auch die kleinste penalty haben
    	//      DONE
    	while (this.currBuckets > this.maxBuckets) {
			// Rufe ReduceBuckets auf dem QTree(Neighbor) auf der die
			// niedrigste Penalty in der simulierten prioQueue hat ...
			double minpenalty = Double.MAX_VALUE;
			QTree minQtree = null;
			for (Neighbor cn :  this.NBQTreeList.keySet()) {
				// QTreeNode cur = ((QRoutingIndex) myPeer.getNeighbor(nid).getIndex()).getNeighborQTree().PrioQueue.peek();
				QTreeNode cur = NBQTreeList.get(cn).getPrioQueue().peek();
				if (cur != null) {
					double cpen = cur.nextToMergePen;				
					if (cpen < minpenalty) {
						minpenalty = cpen;
						// minQtree = ((QRoutingIndex) myPeer.getNeighbor(nid).getIndex()).getNeighborQTree();
						minQtree = NBQTreeList.get(cn);
					}
				}
			}
			
			if (minQtree != null) {
				minQtree.decreaseBuckets();
				//this.currBuckets -= 1;
			}
			else {
				// TODO <DONE> Exception for this case
				
				String errOut="minQtree ist Null -- no buckets to decrease found"+"\n";
				errOut += "this.currBuckets: "+this.currBuckets+", max: "+this.maxBuckets+"\n";
				for (Map.Entry<Neighbor,QTree> currNeigh: this.NBQTreeList.entrySet()){
					errOut += "neighbor: "+currNeigh.getKey().getNeighborID()+"\n";
					errOut += this.getStateStringForNeighbor(currNeigh.getKey())+"\n";
				}
				throw new QTreeException(errOut);
			}
		}
    	assert this.currBuckets == this.getGlobalBucketCount() : 
    		   "currBuckets: "+this.currBuckets+" globalCount: "+this.getGlobalBucketCount();
    }
    
    /**
     * Returns the global Bucket Count
     * @return The Number of Buckets in the whole Routing Index
     */
    private int getGlobalBucketCount(){
    	int sum=0;
    	for (Neighbor n : NBQTreeList.keySet()){
    		QTree q = NBQTreeList.get(n);
    		sum += q.getCurrBuckets();
    	}
    	return sum;
    }

	/* (non-Javadoc)
	 * @see smurfpdms.index.CompoundRoutingIndex#getLocalIndexForNeighbor(smurfpdms.peer.Neighbor)
	 */
	//@Override
	/*public LocalIndex getLocalIndexForNeighbor(Neighbor neighbor) {
		return this.NBQTreeList.get(neighbor);
	}*/

	/* (non-Javadoc)
	 * @see smurfpdms.index.Index#clone()
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Object clone() {
		QRoutingIndex clone = (QRoutingIndex) super.clone();

		// The NBQTreelist is build new
		clone.NBQTreeList = new HashMap<Neighbor, QTree>();
		
		for (Neighbor nb : this.NBQTreeList.keySet()){
			QTree qtClone = this.NBQTreeList.get(nb).clone(); // Clone the Local Index
			qtClone.setParentQRoutingIndex(clone); // Set the new Parent QRouting Inddex
			clone.NBQTreeList.put(nb, qtClone);
		}
		
		return clone;
	}
	
	/**
	 * Updates the Bucket Count of this Routing Index
	 * 
	 * @param amount The Amount to change the Bucketcount(pos. or neg.)
	 */
	protected void updateBucketCount(int amount){
		this.currBuckets += amount;
	}
}