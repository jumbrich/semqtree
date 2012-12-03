/*
 * 
 */
package de.ilmenau.datasum.index.qtree;


import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

import javax.swing.tree.DefaultMutableTreeNode;

import org.jdom.Element;

import de.ilmenau.datasum.config.Parameters;
import de.ilmenau.datasum.exception.QTreeException;
import de.ilmenau.datasum.index.Bucket;
import de.ilmenau.datasum.index.BucketBasedLocalIndex;
import de.ilmenau.datasum.index.IntersectionInformation;
import de.ilmenau.datasum.index.LocalIndex;
import de.ilmenau.datasum.index.QuerySpace;
import de.ilmenau.datasum.index.qtree.penalty.MaximumBucketLength;
import de.ilmenau.datasum.index.qtree.penalty.QPenaltyFunction;
import de.ilmenau.datasum.index.update.UpdateBucket;
import de.ilmenau.datasum.util.DataPoint;
import de.ilmenau.datasum.util.StringHelper;
import de.ilmenau.datasum.util.bigmath.Space;

/**
 * @author matz, hose
 *
 * TODO: to calculate the index quality for evaluation each bucket must contains the indexed points if NetworkConfig.storeDataPointsInBuckets is set
 *			--> see implementation in MultiDimHistogram
 */
public class QTree extends BucketBasedLocalIndex {
	private static final long serialVersionUID = 1L;
	/** Number of Dimensions used */
	private int nmbOfDimensions; 
	/** Maximum Fanout of a Node */
	private int maxFanout;		 
	/** Maximum Buckets this Tree could have */
	private int maxBuckets;
	
	/** counter for giving each bucket a unique ID -- cannot use currNmbOfBuckets 
	 * because that number can be decreased 
	 * starting with 1 because 0 has a special meaning ... (see insertObject(Bucket,ID))*/
	private long bucketIDCounter = 1;

	/** Counter of the Current Number of Buckets*/
	private int currNmbOfBuckets = 0;
	/** */
	private QTreeNode rootNode;

	/** Priority Queue for Bucket Merging */
	private QPriorityQueue prioQueue;
	
	/** 
	 * Number of buckets that would be merged together
	 * This could be set via setMergeCount()
	 * TODO<MM>(DONE): das mal realisieren
	 */
	private int mergeAtOnce = 2;
	
	/** minimum values for each dimension for normalizing */
	private int[] dimSpecMin;
	/** maximum values for each dimension for normalizing */
	private int[] dimSpecMax;
	
	/** the name of the node that elements ({@link #indexOnDimensions}) are indexed */
	private String parentNodeName = null;
	
	/** attribute names of the dimensions which the QTree is defined on */
	private Vector<String> indexOnDimensions;
	
	/** the ID of the peer where the corresponding index is located */
	private String peerID;
	/** the ID of the neighbor (<tt>null</tt> if the index is not part of a routing index) */
	private String neighborID;
	
	/** parameter indicating whether counts for each source should be kept (true) 
	 *    or counts for all contained sources altogether 
	 */
	private boolean storeDetailedCounts = true;
	
	/**
	 * This Class is used by QTreeNode.checkDataAvail for Comparison
	 */
	public class OP {
		/** < */
		public static final int SMALLER=0;
		/** <= */
		public static final int SMALLER_OR_EQUAL=1;
		/** == */
		public static final int EQUAL=2;
		/** >= */
		public static final int GREATER_OR_EQUAL=3;
		/** > */
		public static final int GREATER=4;
	}
	
	/** routing index which this QTree belongs to */
	private QRoutingIndex parentQRoutingIndex = null;

	/** the Penalty Function used for Merging Decisions in this Tree
	 *  default Value is the Maximum Bucket Length
	 */
	private QPenaltyFunction mergePenaltyFunction = new MaximumBucketLength();
	
	
	/** the Penalty Function used for Grouping Decisions in this Tree
	 *  default Value is the Maximum Bucket Length
	 */
	private QPenaltyFunction groupPenaltyFunction = new MaximumBucketLength();

	/** Represents the possible strategies to remove DataItems out of buckets which
	 *  overlap each other
	 * 
	 */
	public enum RemoveStrategy {/**
	 * If Buckets overlap they would be merged together before the Data Item is removed.
	 */
	BUCKET_MERGING,/**
	 * If Buckets overlap the Bucket which Center the minimum Distance to the DataItem
	 * is used first
	 */
	USING_DISTANCETOCENTER, /**
	* If buckets overlap, the updates are shared according to the degree of overlap
	*/
	USING_OVERLAP};
	
	/** The current Strategy used to delete DataItems out of overlapping Buckets
	 * 
	 */
	private RemoveStrategy currentRemoveStrategy = RemoveStrategy.BUCKET_MERGING;
	
	/**
	 * Counts the number of items which should be removed but arent covered by an
	 * existing bucket
	 * 
	 */
	private double removedNotIndexedItemCounter = 0.0;
	
	/** DEBUG: For Checking whether the Insert/Update/Remove Methods are working correctly
	 *  The Flag debugChanges switches between debugging all inserted/removed Points
	 */
	public boolean debugChanges=false;
	/**
	 * One big container saving all ever inserted Points
	 */
	public QTreeBucket debugPointsContainer = null;
	/**
	 * One big container saving all ever removed Points
	 */
	private QTreeBucket debugRemovedPointsContainer = null;
	private Vector<QTreeBucket> BucketList;
	private boolean recomputeBucketList = false;
	
	/**
	 * The constructor for a QTree. Needs the parameter dimension of the data, 
	 * maximum fanout of the tree and the maximum number of buckets in the tree.
	 * 
	 * @param d
	 * @param maxF
	 * @param maxB
	 * @param dimSpecMin
	 * @param dimSpecMax
	 * @param peerID the ID of the peer where the corresponding index is located
	 * @param neighborID the ID of the neighbor (<tt>null</tt> if the index is not part of a routing index)
	 */
	public QTree(int d, int maxF, int maxB, 
			int[] dimSpecMin, int[] dimSpecMax, 
			String peerID, String neighborID, boolean storeDetailedCounts){
		super(IndexType.QTree);
		
		this.nmbOfDimensions=d;
		this.maxFanout = maxF;
		this.maxBuckets = maxB;
		//QTreeNode r = new QTreeNode(this);
		QTreeNode r = null;
		if (dimSpecMin != null && dimSpecMax != null){
			r = new QTreeNode(this, null, 
					dimSpecMin.clone(), dimSpecMax.clone(), 0.0,
					peerID, neighborID, null);
		} else {
			r = new QTreeNode(this, null, 
					dimSpecMin, dimSpecMax, 0.0,
					peerID, neighborID, null);
		}
		this.rootNode = r;
		//this.dimSpecMin = dimSpecMin.clone();
		//this.dimSpecMax = dimSpecMax.clone();
		this.dimSpecMin = dimSpecMin;
		this.dimSpecMax = dimSpecMax;
			
		this.prioQueue = new QPriorityQueue();
		
		this.peerID = peerID;
		this.neighborID = neighborID;

		// Create a Single QTreeBucket for Debugging
		// All Points that are inserted in the Tree would be also inserted into this Container
		// Thus we could find the Source of Errors while Removing  
		if (this.debugChanges) {
			if (!this.storeDetailedCounts){
				this.debugPointsContainer = new QTreeBucket(this,null,this.dimSpecMin,this.dimSpecMax,0,"DE","BUG",0,new HashSet<String>());
				this.debugRemovedPointsContainer = new QTreeBucket(this,null,this.dimSpecMin,this.dimSpecMax,0,"DE","BUG",0,new HashSet<String>());
			} else {
				this.debugPointsContainer = new QTreeBucket(this,null,this.dimSpecMin,this.dimSpecMax,0,"DE","BUG",0,new HashMap<String,Double>());
				this.debugRemovedPointsContainer = new QTreeBucket(this,null,this.dimSpecMin,this.dimSpecMax,0,"DE","BUG",0,new HashMap<String,Double>());				
			}
		}
		
		this.storeDetailedCounts = storeDetailedCounts;
	}

	/**
	 * Returns the number of Fail Removings in this tree
	 * @return The Not Indexed Items which were removed
	 */
	public double getRemoveError() {
		return this.removedNotIndexedItemCounter;
	}
	
	/**
	 * Sets a new Value for the "parentNodeName" : For Evaluation Purposes Only !
	 * @param newParentNodeName The New Name for the Parent Node
	 */
	public void setParentNodeName(String newParentNodeName){
		this.parentNodeName = newParentNodeName;
	}
	
	/**
	 * Return the "parentNodeName" : For Evaluation Purposes Only !
	 * @return The ParentNodeName
	 */
	public String getParentNodeName(){
		return this.parentNodeName;
	}
	
	/**
	 * Sets the MergePenaltyFunction
	 * @param mergePenFunc The Penaltyfunction to use for Merging
	 *
	 */
	public void setMergePenaltyFunction(QPenaltyFunction mergePenFunc){
		this.mergePenaltyFunction = mergePenFunc;
	}
	
	/**
	 * Sets the GroupPenaltyFunction
	 * @param groupPenFunc The PenaltyFunction to use for Grouping
	 *
	 */
	public void setGroupPenaltyFunction(QPenaltyFunction groupPenFunc){
		this.groupPenaltyFunction = groupPenFunc;
	}
	
	/**
	 * Returns this QTree's Penaltyfunction for Merging (Buckets)
	 * @return The Merging Penalty Function
	 */
	public QPenaltyFunction getMergePenaltyFunction(){
		return this.mergePenaltyFunction;
	}
	
	/**
	 * Returns this QTree's Penaltyfunction for Grouping (Nodes)
	 * @return The Grouping Penalty Function
	 */
	public QPenaltyFunction getGroupPenaltyFunction(){
		return this.groupPenaltyFunction;
	}
	
		
	/**
	 * Sets the number of buckets that are merged together in one 
	 * reduction step
	 * @param count The MergeCount that should be used
	 * @throws QTreeException 
	 */
	public void setmergeAtOnce(int count) throws QTreeException{
		if (count > 2) this.mergeAtOnce = count;
		else this.mergeAtOnce = 2;
		if (this.mergeAtOnce > this.maxFanout) throw new QTreeException("MergeCount too high");
	}
	
	/**
	 * Returns the Peer ID
	 * @return The Peer ID this QTree belongs to
	 */
	public String getPeerID(){
		return this.peerID;
	}
	
	/**
	 * Returns the NeighbourID
	 * @return The Neighbour ID this QTree belongs to
	 */
	public String getNeighborID(){
		return this.neighborID;
	}
		
	/**
	 * Inserts a whole bucket into the QTree
	 * 
	 * CHECKED:
	 * 
	 * Returns the change in the number of buckets could be negative or positive
	 * Example:
	 * 
	 * before: 10 buckets
	 * after:  11 buckets
	 * return value: 11-10 = 1 (one bucket more)
	 * 
	 * before: 11 buckets
	 * after:  10 buckets
	 * return value: 10-11 = -1 (one bucket less)
	 * 
	 * @param bucket The bucket that should be inserted
	 * @param setNewLocalBucketID Set to true for automatic Id, else the Id from <i>bucket</i> would be used
	 * 
	 * @return the change in the number of buckets
	 * @throws QTreeException 
	 */
	public int insertDataItem(QTreeBucket bucket,boolean setNewLocalBucketID, boolean insertIntoExistingBucketIfPossible) throws QTreeException {
		// Do some Debugging, keep every single inserted Point in this container ...
		if (this.debugChanges) {
			this.debugPointsContainer.addPoints(bucket.getContainedPoints());
		}
		int nmbOfBucketsBefore = this.currNmbOfBuckets;
		this.rootNode.insertBucket(bucket,setNewLocalBucketID,insertIntoExistingBucketIfPossible);
		int nmbOfBucketsAfter = this.currNmbOfBuckets;
		return (nmbOfBucketsAfter-nmbOfBucketsBefore);
	}
	
	
	/**
	 * Checks if this QTree belongs to a QRoutingIndex
	 * @author hose
	 * @return True if it belongs to a QRoutingIndex
	 */
	public boolean isPartOfQRoutingIndex(){
		if (this.parentQRoutingIndex !=null) {
			return true; 
		}
		return false;
	}
	
	
	/**
	 * Returns the Root Node of this QTree
	 * @return rootNode
	 */
	public QTreeNode getRoot(){
		return this.rootNode;
	}
	
	
	/**
	 * Sets the QRoutingIndex this QTree belongs to
	 * @author hose
	 * @param parent
	 */
	public void setParentQRoutingIndex(QRoutingIndex parent){
		this.parentQRoutingIndex = parent;
	}
	
	
	/**
	 * Reduces the number of Buckets in this QTree
	 * If there is no parent QRoutingIndex then this method operates
	 * locally on this single QTree else the reduceBucket of the according
	 * QRoutingIndex would be called
	 * CHECKED:
	 * @throws QTreeException 
	 * @throws Exception 
	 *
	 */
	public void reduceBuckets() throws QTreeException{
		
		//System.err.println("QTree: "+this.currNmbOfBuckets+", max: "+this.maxBuckets);
		
		// if this is part of a QRoutingIndex we have to call the method of the index
		if (this.parentQRoutingIndex != null){
			if (this.parentQRoutingIndex.checkIfContainsLocalIndex(this)) {
				this.parentQRoutingIndex.reduceBuckets(false);
				return;
			}
		}
		
		// while loop not essentially necessary 
		// --> after inserting a data item 
		// --> there can at maximum be one bucket too much
		// Comment: but if we want to reduce the number of buckets because
		// lack of space or something we just reduce the maxBuckets value
		// and call this method
		while (this.currNmbOfBuckets > this.maxBuckets){
			QTreeNode smallestInnerNode = this.prioQueue.poll();
			if (smallestInnerNode == null) {
				// This happens if the mergeCount in the QTree is 
				// bigger than the fanOut
				throw new QTreeException("QTree.reduceBuckets(): Smallest InnerNode (to merge) == null");				
			}
			smallestInnerNode.reduceBuckets();
		}
		
	}
	
	
	/**
	 * This Method is called by QRoutingIndex.reduceBuckets() if there
	 * have to be buckets reduced in this QTree
	 * CHECKED:
	 * @throws QTreeException 
	 *
	 */
	public void decreaseBuckets() throws QTreeException{
		QTreeNode smallestInnerNode = this.prioQueue.poll();
		smallestInnerNode.reduceBuckets();
	}
	
	
	/**
	 * This Method checks if there's data available according to the given
	 * XPath Expression. The XPath Expression is mapped onto the dimensions
	 * defined by their names on creation time(Constructor).
	 * The Method supports the following comparison operators:
	 * { = , < , > , <= , >= }
	 * There could be more than one limit for one dimension.
	 * Example: expression = "//item[x>5,x<15,y=10]"
	 * This returns true if there is data available with 5 < x < 15, y = 10
	 * 
	 * Note: If there are dimensions in the query that are not indexed,
	 * this method assumes that these values are true ! This was done because
	 * we couldn't exclude a node from beeing queried if we don't know
	 * if there are data items available.
	 * 
	 * TODO<MM>: wieso true, wenn entsprechendes Attribut nicht indexiert 
	 * --> in dem Fall muesste eine besondere Rueckmeldung an den Aufrufer dieser Methode kommen!!! 
	 * Der muss dann entscheiden, was geschehen soll!!!
	 * 
	 * @param expression String representing an XPath expression
	 * 
	 * @return boolean value that is true if this qtree has data accordingly to the Query
	 */
	@Override
	public boolean hasAccordingData(String expression){
		/*
		 Supported Queries(XPath)
		 expression = 
		 "//item" -> true, if index is over "hotel" objects and contains
		 some data about it
		 "//item[x=5]" -> true, if index is over "hotel" objects and there is
		 a bucket that possibly contains a  
		 "//item[x=4,...,z=7]" s.a.
		 
		 "//item[x<5,b>3,c<=67]"
		 
		 Currently only one condition for each dimension is supported.
		 So the following will not be possible:
		 //item[x>4,x<10,y=7]
		 Maybe added later...
		 
		 */
		// TODO<MM> der Parser fuer verschiedene Items wird spaeter
		//      implementiert
		//		jetz erst mal nur die Attribute parsen und nachschauen ob
		//		in Index enthalten
		// TODO<MM>: moegliche Anfragestrings anschauen... Es kann mehrere [] in einem Anfragestring geben
		// TODO<MM>: Du checkst hier nicht auf XML-Attribute, sondern auf Elemente !!! Attribute sind aber auch noch moeglich
		
		// find first [
		// find all ,
		// parse by SplitOperation
		// check if attributes exist, if not return false
		int bopos = expression.indexOf("[");
		if (bopos == -1) return true; // da noch kein Itemname

		int bcpos = expression.indexOf("]");
		String parameterstring = expression.substring(bopos+1,bcpos);
		// remove ' charachters
		parameterstring = parameterstring.replace("'","");
		
		
		String [] keyvals = parameterstring.split(" and "); // one entry for each condition/comparison
		
		String [] dimnames = new String[keyvals.length]; // dimension name used in a comparison
		double [] dimvals = new double[keyvals.length]; // comparison values TODO: double 
		int [] ops = new int[keyvals.length]; // comparison operators

		// extract the different dimensions
		for (int x=0;x<keyvals.length;x++){
			// remove blanks ...
			keyvals[x] =  keyvals[x].replace(" ",""); // delete white spaces
			String [] kvpair;
			String splitop = "";
			int stpos = keyvals[x].indexOf("<");
			int gtpos = keyvals[x].indexOf(">");
			int eqpos = keyvals[x].indexOf("=");
			if (stpos != -1) { // "<"
				if (eqpos != -1) { // "<="
					ops[x] = OP.SMALLER_OR_EQUAL;
					splitop = (stpos < eqpos) ? "<=" : "=<";
				} else { // "<" only
					ops[x] = OP.SMALLER;
					splitop = "<";
				}
			}else { 
				if (gtpos != -1) { // ">" ?
					if (eqpos != -1) { // ">="
						ops[x] = OP.GREATER_OR_EQUAL;
						splitop = (gtpos < eqpos) ? ">=" : "=>";
					}else { // ">" only
						ops[x] = OP.GREATER;
						splitop = ">";
					}
				}else { // "="
					if (eqpos == -1) { // no supported comparison operator found
						try {
							throw new QTreeException("hasAccordingData: No Operator !");
						} catch (QTreeException e) {
							e.printStackTrace();
						}
					} else {
						ops[x] = OP.EQUAL;
						splitop = "=";
					}
				}    				
			}
			
			// System.out.println(splitop+" "+ops[x]);
			kvpair = keyvals[x].split(splitop);    			
			dimnames[x] = kvpair[0];
			//dimvals[x] = Integer.parseInt(kvpair[1]);;
			dimvals[x] = Double.parseDouble(kvpair[1]);
		} // end for
		
		
		try {
			return this.rootNode.checkDataAvail(dimnames,ops,dimvals);
		} catch (QTreeException e) {
			e.printStackTrace();
			return true;
		}
	}
	
	
	/* (non-Javadoc)
	 * @see smurfpdms.index.Index#clone()
	 */
	@SuppressWarnings("unchecked")
	@Override
	public QTree clone() {
    	QTree clone = (QTree) super.clone();
    	
    	// clone "simple"/nonrecursive Types
    	clone.dimSpecMax = this.dimSpecMax.clone();
    	clone.dimSpecMin = this.dimSpecMin.clone();
    	
    	clone.groupPenaltyFunction = this.groupPenaltyFunction.clone();
		clone.mergePenaltyFunction = this.mergePenaltyFunction.clone();

    	if (this.indexOnDimensions != null)
    		clone.indexOnDimensions = (Vector<String>) this.indexOnDimensions.clone();
    	
    	if (this.parentNodeName != null)
    		clone.parentNodeName = this.parentNodeName;
    
    	// Create a Map to associate old Nodes to new Nodes
    	HashMap<QTreeNode,QTreeNode> assocTbl = new HashMap<QTreeNode, QTreeNode>();
    	
    	// First clone RootNode flat
    	clone.rootNode = this.rootNode.clone();
    	
    	// Add rootNode to HashMap
    	assocTbl.put(this.rootNode, clone.rootNode);
    	
    	// Clone the Structure
    	clone.rootNode.cloneStructure(clone,null,assocTbl);
    	
    	// Finally the PriorityQueue is cloned and all entrys are replaced by the new 
    	// Nodes using the association Table
    	clone.prioQueue = this.prioQueue.clone();
    	clone.prioQueue.rebuild(assocTbl);
    	
    	
     	return clone;
    } 
    
    
    /**
     * 
     */
    public boolean isEmpty(){
    	return this.rootNode.getChildren().isEmpty();
    }
    
    
	/**
	* abstract method for getting the state string of the Index object.
	*/
    public String getStateString(){
    	String ss;
    	// Dims,maxBuckets,currNmbOfBuckets;
    	ss = "Type: "+this.type+"\n";
    	ss += "  Dimensions: "+this.nmbOfDimensions+"\n";
    	ss += "  MaxBuckets: "+this.maxBuckets+"\n";
    	ss += "  CurrentBuckets: "+this.currNmbOfBuckets+"\n";
    	ss += "  Dimensions: "+this.indexOnDimensions+"\n";
    	ss += "  dimSpecMin: "+Arrays.toString(this.dimSpecMin);
    	ss += "  dimSpecMax: "+Arrays.toString(this.dimSpecMax);
    	// Tree Structure
    	ss += this.rootNode.getStructureString("    ");
    	return ss;
    }
    
    /**
     * Returns the Maximum Number of Buckets 
     * @return The Maximum Bucket Count
     */
    public int getMaxBuckets(){
    	return this.maxBuckets;
    }
	
    /**
     * 
     * @return The number of buckets currently existing in the Tree
     */
    public int getCurrBuckets(){
    	return this.currNmbOfBuckets;
    }
    
    /**
     * Return the DimSpecMin Values for each Dimension 
     * @return Array cotaining the DimSpecMin Values
     */
	public int[] getDimSpecMin(){
		return this.dimSpecMin;
	}

    /**
     * Return the DimSpecMax Values for each Dimension 
     * @return Array cotaining the DimSpecMax Values
     */	
	public int[] getDimSpecMax(){
		return this.dimSpecMax;
	}
	
	/**
	 * Returns the maximum Fanout set for this QTree
	 * @return The Maximum Fanout
	 */
	public int getMaxDegree(){
		return this.maxFanout;
	}
	
	/**
	 * Returns the Number of Dimensions used
	 * @return The Number of Dimensions
	 */
	public int getNmbOfDimensions(){
		return this.nmbOfDimensions;
	}
	
	/**
	 * Sets indexOnDimensions <br>
	 * Note: Shouldn't be used, only for Testing purposes !<br>
	 * 
	 * @param newIndexOnDimensions - New List of Dimension Names
	 */
	public void setIndexOnDimensions(Vector<String> newIndexOnDimensions){
		this.indexOnDimensions = newIndexOnDimensions;
	}
	
	/**
	 * Increment the Current Bucket Counter
	 * If this is Part of Routing Index, the Bucket Count of the Routing Index
	 * has to be incremented, too
	 *
	 */
	public void incCurrBuckets(){
		this.currNmbOfBuckets++;
		if (this.parentQRoutingIndex != null){
			if (this.parentQRoutingIndex.checkIfContainsLocalIndex(this)){
				this.parentQRoutingIndex.updateBucketCount(+1);
			}
		}
	}
	
	/**
	 * Returns the Priority Queue used by this QTree
	 * @return The Priority Queue
	 */
	public QPriorityQueue getPrioQueue(){
		return this.prioQueue;
	}
	
	/**
	 * Assigns Attribute Names to each used Dimension
	 * These Names are used for Queries in the
	 * hasAccrdingData Method
	 * @param attributeNames A List of Strings containing the Names
	 */
	public void setAttributeNames(Vector<String> attributeNames) {
		this.indexOnDimensions = attributeNames;
	}
	
	/**
	 * CHECKED: <br>
	 * Reduces the Number of Buckets that are present in the QTree
	 *  
	 */
	public void decCurrBucketCount(){
		this.currNmbOfBuckets--;
		if (this.parentQRoutingIndex != null){
			if (this.parentQRoutingIndex.checkIfContainsLocalIndex(this)){
				this.parentQRoutingIndex.updateBucketCount(-1);
			}
		}
	}
	
	
	/**
	 * Prints a Debug Output of the QTree 
	 *
	 */
	public void printme(){
		this.rootNode.print("  ");
	}

	/**
	 * constructs QTrees according to the given parameters
	 *  --> for creating local indexes
	 *  
	 * @param data 
	 * @param indexOnDimensions
	 * @param dimSpecMin
	 * @param dimSpecMax
	 * @param maxFanout 
	 * @param maxNmbOfBuckets 
	 * @param peerID the ID of the peer that this QTree belongs to as local index 
	 * 		--> necessary to build a globally unique bucket ID
	 * @return The Created QTree
	 * @throws Exception 
	 */
	public static QTree createIndex(Element data,	    		 
	    		Vector<String> indexOnDimensions, 
	    		int[] dimSpecMin,
	    		int[] dimSpecMax,
	    		int maxFanout,
	    		int maxNmbOfBuckets,
	    		String peerID, 
	    		boolean storeDetailedCounts) throws Exception{

		int nmbOfDimensions = indexOnDimensions.size();
		// index for current peer
		QTree LRI = new QTree(nmbOfDimensions, maxFanout, 
				maxNmbOfBuckets, dimSpecMin, dimSpecMax, 
				peerID, null, storeDetailedCounts);
		LRI.indexOnDimensions = indexOnDimensions;
		
		for ( Object currChildObject: data.getChildren()){
			Element currChild = (Element) currChildObject;
			if (LRI.parentNodeName == null) {
				LRI.parentNodeName = currChild.getName();
			}
			//System.out.println("ITEM: "+currChild.getName());
		 
			// create a data item to insert
			int[] currElement = new int[nmbOfDimensions];
			byte count = 0;
			for (String attr: indexOnDimensions){
				// TODO: <MM> Begrenzung auf Integer Werte entfernen
				currElement[count]=Integer.parseInt(currChild.getChildText(attr));
				count++;
			}
			try {
				//System.out.println("ITEM: "+currChild.getName()+", "+Arrays.toString(currElement));
				LRI.insertDataItem(currElement, peerID);
				// hack for SemanticQTree project !!!!
				
				//System.out.println(LRI.getStateString()+"\n\n");
			} catch (Exception e) {
				// ignore invalid data
			}
		}
		
		//LRI.setGlobalBucketIDForAllBuckets(peerID);
		
		/*LRI.printme();
		System.out.println("\n");*/
		
		return LRI;
	 }
	
	/**
	 * CHECKED:
	 * 
	 * @return A List wich contains all(and only) the Buckets from this QTree
	 */
	public Vector<QTreeBucket> getAllBuckets() {
	    if(recomputeBucketList) computeBucketList();
	    return BucketList;
	}
	
	private void computeBucketList(){
//	    System.out.println("[QTree] compute new bucket list!");
	    
	    BucketList = new Vector<QTreeBucket>();
	    this.rootNode.getAllBuckets(BucketList);
	    recomputeBucketList = false;
	}
	
	public HashMap<String, Bucket> getAllBucketsAsHashMapGlobalID() {
		HashMap<String, Bucket> bucketList = new HashMap<String, Bucket>();
		this.rootNode.getAllBucketsInHashMapGlobalID(bucketList);
		return bucketList;
	}
    
    /**
     * returns a unique ID for each bucket that is created
     * 
     * @return The Unique ID
     */
    public long getQTreeUniqueBucketID(){
    	long id = this.bucketIDCounter;
    	this.incQTreeUniqueBucketIDCounter();
    	return id;
    }
    
    
    /**
     * @author hose
     *
     */
    public void incQTreeUniqueBucketIDCounter(){
    	this.bucketIDCounter++;
    }
    
    /**
     * Returns the current value of the UniqueBucketID Counter without increasing it
     * @return The current value of the UniqueBucketID Counter
     */
    public long readQTreeUniqueBucketID(){
    	return this.bucketIDCounter;
    }
    
    /**
     * Sets the Value of the UniqueBucketID Counter
     * Note ! The new Value has to be bigger than the existing one !
     * @param newValue The new Value for the ID Counter
     * @throws Exception If the new Value violates the Unique Rule
     */
    public void setQTreeUniqueBucketIDCounter(long newValue) throws Exception{
    	if (this.bucketIDCounter > newValue) {
    		throw new Exception("QTree.setQTreeUniqueBucketIDCounter: Unique Property of ID Counter violated");
    	}
    	this.bucketIDCounter = newValue;
    }
    
    /**
     * Returns the Routing Index this QTree belongs to
     * @author hose
     * 
     * @return The Parent Routing Index
     */
    public QRoutingIndex getParentQRoutingIndex(){
    	return this.parentQRoutingIndex;
    }
    
    
    /** 
     * Computes the maximum bucket length by computing it for all 
     * buckets and returning the maximum length that was found.
     * 
     * @author hose
     * 
     * @return The Maximum Bucket Length
     */
    public double computeQTreeMaxBucketLength(){
    	double maxLength = 0.0;
    	
    	Vector<QTreeBucket> allBuckets = getAllBuckets();
    	MaximumBucketLength mbl = new MaximumBucketLength();
    	for (QTreeBucket currBucket: allBuckets){
    		double currMaxLength = mbl.calculatePenalty(this.dimSpecMin, 
    													this.dimSpecMax, 
    													currBucket);
    		if (currMaxLength > maxLength){
    			maxLength = currMaxLength;
    		}
    	}
    	
    	return maxLength;
    }


	/* (non-Javadoc)
	 * @see smurfpdms.index.BucketBasedLocalIndex#getAttributeNames()
	 */
	@Override
	public Vector<String> getAttributeNames() {
		return this.indexOnDimensions;
	}


	/* (non-Javadoc)
	 * @see smurfpdms.index.BucketBasedLocalIndex#insertDataItem(int[])
	 */
	@Override
	public void insertDataItem(int[] coordinates, String sourceID) throws Exception {
		// Do some Debugging, keep every single inserted Point in this container ...
		if (this.debugChanges) {
			this.debugPointsContainer.addPoint(
				new DataPoint(coordinates, this.getParentNodeName(), this.getAttributeNames())
			);
		}
		this.rootNode.insertDataItem(coordinates, sourceID);
		assert this.currNmbOfBuckets == this.getNmbOfBuckets();
		recomputeBucketList = true;
	}
	
	public boolean storeDetailedCounts(){
		return this.storeDetailedCounts;
	}

	/* (non-Javadoc)
	 * @see smurfpdms.index.BucketBasedLocalIndex#removeDataItem(int[])
	 */
	@Override
	public void removeDataItem(int[] coordinates) throws Exception {
		// Due to the same procedures for Removing Data Items or Buckets
		// We create a Dummy Bucket and remove that ...
		QTreeBucket dummy;
		if (!this.storeDetailedCounts){
			dummy = new QTreeBucket(null,null,coordinates,coordinates,1,null,null,0, new HashSet<String>());
		} else {
			dummy = new QTreeBucket(null,null,coordinates,coordinates,1,null,null,0, new HashMap<String,Double>());
		}
		// only for evaluation store the points in the bucket!
		if (Parameters.storeDataPointsInBuckets) {
			dummy.addPoint(
				new DataPoint(coordinates, this.getParentNodeName(), this.getAttributeNames())
			);
		}
		try {
			this.removeDataItem(dummy);
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println(StringHelper.arrayToString(coordinates));
			System.err.println(this.getStateString());			
		}
	}
	
	/**
	 * Check whether the given coordinates are the same
	 * @param coordsA First Set of Coordinates
	 * @param coordsB Second Set of Coordinates
	 * @return True, if the coordinates are the same
	 */
	private boolean compareCoords(int [] coordsA, int [] coordsB){
		for (int i=0;i<coordsA.length;i++){
			if (coordsA[i] != coordsB[i]) return false;
		}
		return true;
	}
	
	/**
	 * Returns the times this point was inserted
	 * @param coords The coordinates of the point to check
	 * @return The number of ever made insertions of this Point
	 */
	private int checkInsertedTimes(int [] coords) {
		int ret=0;
		for (DataPoint dp : debugPointsContainer.getContainedPoints()){
			int [] currCoords = dp.getCoordinates();
			if (compareCoords(coords,currCoords)) ret += 1;
		}
		return ret;
	}

	/**
	 * Returns the times this point was removed
	 * @param coords The coordinates of the point to check
	 * @return The number of ever made removes of this Point
	 */
	private int checkRemovedTimes(int [] coords) {
		int ret=0;
		for (DataPoint dp : debugRemovedPointsContainer.getContainedPoints()){
			int [] currCoords = dp.getCoordinates();
			if (compareCoords(coords,currCoords)) ret += 1;
		}
		return ret;
	}
	
	/**
	 * Removes a Bucket using the "Bucket Merging" Strategy
	 * @param remBucket The Bucket to Remove
	 * @param responsibleBuckList The List of responsible Buckets
	 * @param RespBucketsCompletelyEncloseRemBucket encodes if the buckets given in responsible buckets completely enclose remBucket's MBB
	 * @return returns the number of records that could not be removed
	 */
	private void removeDataItemApplyingBucketMerging(QTreeBucket remBucket,
													 Vector<QTreeBucket> responsibleBuckList) throws Exception{
		QTreeBucket mergeBucket = null;
		
		// If the size of the "responsibleBuckList" is greater than one,
		// all Elements have to be merged into one single Bucket
		
		if (Parameters.storeDataPointsInBuckets) {
			System.out.println("+++ removeDataItemApplyingBucketMerging +++");
			System.out.println("** Before Removing: **");
			System.out.println("responsibleBuckList.size = "+responsibleBuckList.size());
			System.out.println("QTree.NumbOfBuckets before removing = "+this.currNmbOfBuckets);
			System.out.println("Bucket/Point to remove:");
			System.out.println(remBucket);
			System.out.println("ReduceCount = "+remBucket.getCount());
			System.out.println("DataPointCount = "+remBucket.getContainedPoints().size());
		}
		
		//System.out.println("RespBucketList: "+responsibleBuckList.size()+" entries, #buckets "+this.getAllBuckets().size()+" (Phase Enclose "+RespBucketsCompletelyEncloseRemBucket+")");
		
		if (responsibleBuckList.size() > 1){
			// Find the Bucket with the Minimum Local Bucket ID
			// System.out.println("****   BEFORE MERGING  ****");
	        // System.out.println(this.getStateString());
			long minLocalBucketID = Long.MAX_VALUE;
	        for (QTreeBucket b : responsibleBuckList){
				if (b.getLocalBucketID() < minLocalBucketID) {
					minLocalBucketID = b.getLocalBucketID();
					mergeBucket = b;
				}
			}
			
			// cloning it because it is going to be removed from the tree
			QTreeBucket tempClone = mergeBucket.clone();
			tempClone.setCount(mergeBucket.getCount()); // just to be sure
			tempClone.setParent(null);
			tempClone.setLocalBucketID(mergeBucket.getLocalBucketID());
			tempClone.setGlobalBucketID(mergeBucket.getGlobalBucketID());
			
			// remove from the List
			responsibleBuckList.remove(mergeBucket);
			
			// delete from the tree (includes rebalancing)
			mergeBucket.removeDataItem(mergeBucket.clone());
			
			mergeBucket = tempClone;
			
			// go through the list of the other responsible buckets, merge them into mergeBucket, and remove them from the tree
			for (QTreeBucket b : responsibleBuckList) {
				QTreeBucket c = b.clone();
				
				// enlarge mergeBucket's boundaries and increase count
				mergeBucket.enlargeBounds(c);
				mergeBucket.setCount(mergeBucket.getCount()+c.getCount());
				
				// delete b from the tree
				try {
					// First add all Points to the new MergeBucket (for Evaluation Purposes Only)
					if (Parameters.storeDataPointsInBuckets){
						mergeBucket.addPoints(b.getContainedPoints());
					}
					// Remove c from Tree(Rebalancing is done automatically)
					b.removeDataItem(c);
				} catch (QTreeException e) {
					// This isn't possible
					System.err.println("Impossible Exception !!");
					e.printStackTrace();
				}
			}
			
			// reinsert mergeBucket into the tree
			this.insertDataItem(mergeBucket, false, true);
			
			// as mergeBucket is inserted as a new bucket instance we need to get the reference to the correct object
			Vector<QTreeBucket> bucketList = this.getAllBuckets();
			for (QTreeBucket b: bucketList){
				if (b.getLocalBucketID() == mergeBucket.getLocalBucketID()){
					mergeBucket = b;
					break;
				}
			}
			
			// Afterwards we have to update the Priority Queue --> NullPointerException because mergeBuckets reference to parent may not exist
			//mergeBucket.parent.updatePrioQueue();
			
			// Now we have a bigger Bucket without overlapping Regions
			// Thus, we can remove the records from it safely and correctly
			try {
				//System.out.println(">1 RespBucket: Count in Merge Bucket before Removing = "+mergeBucket.getCount()+", #buckets "+this.getAllBuckets().size());
				if (Parameters.storeDataPointsInBuckets) {
					System.out.println("DataPointCount before Removing= "+mergeBucket.getContainedPoints().size());
				}
				double notDeleted = mergeBucket.removeDataItem(remBucket);
				remBucket.setCount(notDeleted);
				
				//System.out.println(">1 RespBucket: Count in Merge Bucket after Removing = "+mergeBucket.getCount()+", #buckets "+this.getAllBuckets().size());
				
				this.removedNotIndexedItemCounter += notDeleted;
				
			} catch (QTreeException e) {
				double errCnt = Double.parseDouble(e.getMessage());
				this.removedNotIndexedItemCounter += errCnt;
			}
			
		}
		else {
			
			if (responsibleBuckList.size() == 1) {
				try {
					
					mergeBucket = responsibleBuckList.get(0);
					if (mergeBucket == null) {
						// it should be impossible to reach this statement
						System.out.println("mergeBucket == null !!");
					}
					
					//System.out.println("=1 RespBucket: Count in Merge Bucket before Removing = "+mergeBucket.getCount()+", #buckets "+this.getAllBuckets().size());
					
					if (Parameters.storeDataPointsInBuckets) {
						System.out.println("DataPointCount before Removing= "+mergeBucket.getContainedPoints().size());
					}
					
					// remove the number of records from the one single bucket that encloses or intersects remBucket
					double notDeleted = mergeBucket.removeDataItem(remBucket);
					remBucket.setCount(notDeleted);
					
					//System.out.println("=1 RespBucket: Count in Merge Bucket after Removing = "+mergeBucket.getCount()+", #buckets "+this.getAllBuckets().size());
					
					this.removedNotIndexedItemCounter += notDeleted;
					
				} catch (QTreeException e) {
					double errCnt = Double.parseDouble(e.getMessage());
					this.removedNotIndexedItemCounter += errCnt;
				}
				
			} 
			else 
			{
				// This shouldn't happen with this stratey if all "removeItems"
				// are added before they are removed again
				this.removedNotIndexedItemCounter += remBucket.getCount();
				//System.err.println("ErrorCounter = "+this.removedNotIndexedItemCounter);
				if ((Parameters.storeDataPointsInBuckets) && (this.debugChanges)) {
					//System.err.println("Checking whether Point was ever inserted ...");
					if (remBucket.getCount() == 1){
						for (int i=0;i<remBucket.getLowerBoundaries().length;i++){
							if (remBucket.getLowerBoundaries()[i] != remBucket.getUpperBoundaries()[i]){
								System.out.println("No single DataPoint !");
								System.out.println(remBucket);
								return;
							}
						}
						int [] coordinates = remBucket.getLowerBoundaries();
						int insTimes = checkInsertedTimes(coordinates);
						int remTimes = checkRemovedTimes(coordinates);
						if (insTimes == remTimes){
							System.out.println("The Point was already removed !");
						}
						if (insTimes > remTimes){
							System.out.println("ERROR !! The Point must be still indexed !!");
						}
					}
					else {
						System.out.println("---------");
						System.out.println(remBucket);
						System.out.println(this);
						System.out.println("---------");
					}
				} 
				else {
					// System.err.println("Network Config wrong");
				}
				return;
			}
		}
		
		if (Parameters.storeDataPointsInBuckets) {
			System.out.println("** After Removing **");
			System.out.println("QTree.NumbOfBuckets after removing = "+this.currNmbOfBuckets);
			System.out.println("Count after Removing = "+mergeBucket.getCount());
			System.out.println("DataPointCount after Removing = "+mergeBucket.getContainedPoints().size());
			assert mergeBucket.getCount() == mergeBucket.getContainedPoints().size() : "Different Counts detected";
		}
	}
	
	
	/**
	 * Removes a Bucket using the "MinDistance to Bucket Center" Strategy
	 * 
	 * @param remBucket The Bucket to Remove
	 * @param responsibleBuckList The List of responsible Buckets
	 */
	private void removeDataItemApplyingUsingDistanceToNodeCenter(QTreeBucket remBucket,
													 Vector<QTreeBucket> responsibleBuckList){
		
		if (responsibleBuckList.size() > 0) {
			// collect the distances of the bucket centers to remBucket's center
			double [] distances = new double[responsibleBuckList.size()];
			
			// collect the buckets corresponding to the distances (same position encodes correspondence)
			QTreeBucket [] buckRefs = new QTreeBucket[responsibleBuckList.size()];
			
			// at the first position is always that bucket with the least distance
			
			// initialize with the first bucket
			distances[0] = responsibleBuckList.firstElement().getDistanceBucketCenterToNodeCenter(remBucket);
			buckRefs[0] = responsibleBuckList.firstElement();
			
			// compute the distances of the remaining buckets and sort them accordingly
			if(responsibleBuckList.size()>1){
				
				// sort all responsible Buckets according to their distance to remBucket's center
				for (int i=1;i<responsibleBuckList.size();i++){
					// calculate penalty
					double cpen = responsibleBuckList.get(i).getDistanceBucketCenterToNodeCenter(remBucket);
					// compare to already processed buckets
					int j=i-1;
					while ((j>=0) && (distances[j]>cpen)) {
						// shift all bigger elements to the right
						distances[j+1] = distances[j];
						buckRefs[j+1] = buckRefs[j];
						j--;
					};
					distances[j+1] = cpen;
					buckRefs[j+1] = responsibleBuckList.get(i);
				}
			}
			
			/*String debugOutput = "";
			for (int i=0;i<distances.length;i++){
				debugOutput+="["+distances[i]+":"+buckRefs[i].getCount()+"],";
			}
			System.out.println(debugOutput);*/
				
			// go through the sorted list of buckets and delete the records
			// remove as much from each bucket as possible
			int i=0;
			// remember the number of buckets to delete
			double currRemCount = remBucket.getCount();
			// while there are still some buckets to remove and buckets to delete from left
			while ((currRemCount > 0.5) && (i < buckRefs.length)) {
				// if we need to delete more records than contained in the current bucket
				if (currRemCount >= buckRefs[i].getCount()) {
					try {
						currRemCount -= buckRefs[i].getCount();
						buckRefs[i].removeDataItem(buckRefs[i].clone());
					} catch (QTreeException e) {
						// this should never happen ...
						e.printStackTrace();
					}
				} else {
					// the number of records contained in the current bucket is greater than the number of records we need to delete
					remBucket.setCount(currRemCount);
					try {
						buckRefs[i].removeDataItem(remBucket);
					} catch (QTreeException e) {
						e.printStackTrace();
					}
					currRemCount = 0;
					break;
				}
				i++;
			}
			remBucket.setCount(currRemCount);
			
			//if (currRemCount > 0.5) {
				// Not all DataItems have been removed
				//System.err.println("!!!!!!!!HELP");
				/*System.out.println("currRemCount: "+currRemCount);
				Vector<QTreeBucket> buckList = this.getAllBuckets();
				Vector<QTreeBucket> responsibleBuckList2 = new Vector<QTreeBucket>();
				
				
				try {
					for (int j=0;j<buckList.size();j++){
						// if i'th entry contains remBucket's MBB -- put that entry into the list of responsible buckets
						if (buckList.get(j).overlaps(remBucket)){
							responsibleBuckList2.add(buckList.get(j));
						}
					}
				} catch (QTreeException e) {
					e.printStackTrace();
				}
				System.out.println(responsibleBuckList2.size()+": "+responsibleBuckList2);
				System.exit(0);
				this.removedNotIndexedItemCounter += currRemCount;*/
			//}
		}
	}
	
	
	/**
	 * Removes a bucket sharing the deletion according to the amount of overlap
	 * 
	 * ATTENTION: all buckets in responsibleBuckList need to overlap!!! There is no extra check!!!
	 * 
	 * @param remBucket The bucket to remove
	 * @param responsibleBuckList The list of responsible buckets
	 */
	private void removeDataItemApplyingUsingOverlap(QTreeBucket remBucket,
													 Vector<QTreeBucket> responsibleBuckList){
		
		if (responsibleBuckList.size() > 0) {
			// collect the overlaps (volume) of the buckets with remBucket
			double [] overlappingVolumes = new double[responsibleBuckList.size()];
			
			// sum of all overlapping volumes
			double sumVolumes = 0.0;
			
			// collect the buckets corresponding to the distances (same position encodes correspondence)
			QTreeBucket [] buckRefs = new QTreeBucket[responsibleBuckList.size()];
			
			for (int i=0;i<responsibleBuckList.size();i++){
				buckRefs[i] = responsibleBuckList.elementAt(i);
				
				//String debugOutput = "currRespBucket: "+buckRefs[i].getBoundsString();
				
				// initialize boundaries for overlapping region
				int[] overlapMinBounds = new int[buckRefs[i].getLowerBoundaries().length];
				int[] overlapMaxBounds = new int[buckRefs[i].getUpperBoundaries().length];

				// determine overlapping region
				for (int dim=0;dim<remBucket.getLowerBoundaries().length;dim++){
					// if the lower boundary of remBucket is smaller, then take the lower boundary of buckRefs[i] to determine the overlapping region
					if (remBucket.getLowerBoundaries()[dim] < buckRefs[i].getLowerBoundaries()[dim]){
						overlapMinBounds[dim] = buckRefs[i].getLowerBoundaries()[dim];
					} else {
						overlapMinBounds[dim] = remBucket.getLowerBoundaries()[dim];
					}
					
					if (remBucket.getUpperBoundaries()[dim] < buckRefs[i].getUpperBoundaries()[dim]){
						overlapMaxBounds[dim] = remBucket.getUpperBoundaries()[dim];
					} else {
						overlapMaxBounds[dim] = buckRefs[i].getUpperBoundaries()[dim];
					}
				}
				
				//debugOutput += "Overlapping Region: L["+overlapMinBounds[0]+","+overlapMinBounds[1]+"] H["+overlapMaxBounds[0]+","+overlapMaxBounds[1]+"]";
				
				// determine overlap volume
				// it is at least one because we wouldn't be given a non-overlapping bucket is input 
				double volume=1;
				for (int dim=0;dim<overlapMinBounds.length;dim++){
					volume *= overlapMaxBounds[dim] - overlapMinBounds[dim] + 1;
				}
				
				overlappingVolumes[i] = volume;
				
				sumVolumes += volume;

				//debugOutput += " volume: " + volume;
				//System.out.println(debugOutput);
				
			}
			
			// holds the number of deletions for each responsible bucket
			double[] deleteDistribution = new double[buckRefs.length];
			
			// no division by 0 possible, we have at least 1 responsible bucket
			double factor = remBucket.getCount() / (sumVolumes);
			
			//String debugOutput = "delete distribution: ";
			// compute the number of deletions for each responsible bucket
			for (int i=0;i<buckRefs.length;i++){
				deleteDistribution[i] = factor * overlappingVolumes[i];
				//debugOutput += deleteDistribution[i]+", ";
			}
			//System.out.println(debugOutput);
			
			
			double compensationTooMuchDeleted = 0.0;
			
			// buckets that are still there after deletion of the records according to distribution
			Vector<Integer> remainingOverlappingBucketPositions = new Vector<Integer>();
			
			for (int i=0; i< deleteDistribution.length;i++){
				// if more should be deleted from a bucket than it contains --> correct this
				if (deleteDistribution[i] > buckRefs[i].getCount()){
					deleteDistribution[i] = buckRefs[i].getCount();
				} 
				if (buckRefs[i].getCount() - deleteDistribution[i] >= 0.5){
					remainingOverlappingBucketPositions.add(i);
				} else {
					compensationTooMuchDeleted += buckRefs[i].getCount() - deleteDistribution[i];
				}
			}
			//System.out.println("compensationTooMuchDeleted: "+compensationTooMuchDeleted+", removedNotIndexedItemCounter: "+this.removedNotIndexedItemCounter);
			
			
			//String nmbOfBuckets = "buckets before: "+this.currNmbOfBuckets;
			
			//double failure = 0.0;
			double oldDeleteCount = remBucket.getCount();
			// go through the list of deletions and delete the data
			for (int i=0;i<deleteDistribution.length;i++){
				remBucket.setCount(deleteDistribution[i]);
				oldDeleteCount -= deleteDistribution[i];
				try {
					QTreeBucket bucketToRemove;
					
					if (!this.storeDetailedCounts){
						bucketToRemove = new QTreeBucket(null,null,remBucket.getLowerBoundaries().clone(),
		    				remBucket.getUpperBoundaries().clone(),deleteDistribution[i],null,null,0,new HashSet<String>());
					} else {
						bucketToRemove = new QTreeBucket(null,null,remBucket.getLowerBoundaries().clone(),
			    			remBucket.getUpperBoundaries().clone(),deleteDistribution[i],null,null,0,new HashMap<String,Double>());						
					}
					buckRefs[i].removeDataItem(bucketToRemove);
					//double errorDiff = buckRefs[i].removeDataItem(remBucket);
				} catch (QTreeException e) {
					// this should never happen ...
					e.printStackTrace();
				}
				//System.out.println("nach Loeschen "+buckRefs[i].getBoundsString());
			}
			remBucket.setCount(oldDeleteCount);
			
			/*if (Math.abs(oldDeleteCount)<0.000001){
				oldDeleteCount=0.0;
			}*/
			
			//nmbOfBuckets += ", after: "+this.currNmbOfBuckets;
			//System.out.println(nmbOfBuckets);
			
			// floating point error and deletion error
			if (compensationTooMuchDeleted>0){
				this.removedNotIndexedItemCounter -= compensationTooMuchDeleted;
			}
			
			// if we wanted to delete more records from a bucket (or buckets) than it (they) contained
			if ((oldDeleteCount>0.001)){
				//System.err.println("HElP!!!!!");
				//System.err.println("records to delete: "+oldDeleteCount);
				//System.err.println("compensationTooMuchDeleted: "+compensationTooMuchDeleted);
				//System.err.println("removedNotIndexedItemCounter: "+this.removedNotIndexedItemCounter);
				//System.out.println(this.getStateString());
				
				
				// if we cannot compensate with too many already deleted records
				if (this.removedNotIndexedItemCounter + oldDeleteCount > 0){
				//if (oldDeleteCount - compensationTooMuchDeleted > 0){
					
					//System.out.println("RECURSIVE");
					//call this method recursively
					Vector<QTreeBucket> newRespBuckets = new Vector<QTreeBucket>();
					for (Integer t:remainingOverlappingBucketPositions){
						newRespBuckets.add(buckRefs[t]);
						//System.out.println(buckRefs[t].getCount());
					}
					
					if (newRespBuckets.isEmpty()){
						//this.removedNotIndexedItemCounter += remBucket.getCount();
						//System.err.println("cannot delete that");
					} else {
						removeDataItemApplyingUsingOverlap(remBucket, newRespBuckets);
					}
					
				} else {
					this.removedNotIndexedItemCounter += oldDeleteCount;
					//System.err.println("compensated");
					//System.err.println("removedNotIndexedItemCounter: "+this.removedNotIndexedItemCounter);
					
				}
			} else {
				// oldDeleteCount should always be very close to 0
				this.removedNotIndexedItemCounter += oldDeleteCount;
				remBucket.setCount(0.0);
			}
			
		} 
	}
	
	
	/**
	 * This method removes a whole Bucket from the tree if it is contained. <br>
	 * This could be a Point bucket, too. 
	 * After reducing the count, the changes will be propagated bottom-up through
	 * the Tree until the rootNode is reached.
	 * If one Bucket has to be deleted, the boundaries of the parent Nodes will be
	 * adjusted. If the DataItem <i>removeBucket</i> is located in an area where Buckets 
	 * overlap the solving Strategy <i>currentRemoveStrategy</i> is used. 
	 * <br>
	 * // TODO: develop strategies for shrinking buckets
	 * 
	 * @author Matz
	 * @param remBucket 
	 * 
	 * @throws Exception 
	 */
	public void removeDataItem(QTreeBucket remBucket) throws Exception {
		
		// Only for Debugging
		if (this.debugChanges) {
			this.debugRemovedPointsContainer.addPoints(remBucket.getContainedPoints());
		}
		
		// find all buckets in this QTree whose MBBs completely ENCLOSE remBucket
		Vector<QTreeBucket> buckList = this.getAllBuckets();
		Vector<QTreeBucket> responsibleBuckList = new Vector<QTreeBucket>();
		
		// IMPORTANT Notice: it is definitly wrong to delete first from buckets that totally enclose the region to delete
		// --> the removed buckets could have also been inserted into the other buckets -- so this would lead to a failure !!!
		
		/*for (int i=0;i<buckList.size();i++){
			// if i'th entry contains remBucket's MBB -- put that entry into the list of responsible buckets
			if (buckList.get(i).isResponsibleFor(remBucket)){
				responsibleBuckList.add(buckList.get(i));
			}
		}
		
		//if (responsibleBuckList.isEmpty()){
		//	for (QTreeBucket b: buckList){
		//		System.out.println("region of existing bucket: "+b.getBoundsString());
		//	}
		//}
		
		switch (this.currentRemoveStrategy) {
		case BUCKET_MERGING:
			// remBucket after calling this method has a counter representing the number of non-deleted records
			removeDataItemApplyingBucketMerging(remBucket,responsibleBuckList,true); // true encodes that responsibleBuckets contains only Buckets that completely enclose remBucket
			break;
		case USING_DISTANCETOCENTER:{
			removeDataItemApplyingUsingDistanceToNodeCenter(remBucket,responsibleBuckList);
			break;
		}
		default:
			removeDataItemApplyingBucketMerging(remBucket,responsibleBuckList,true);
			break;
		}
		//System.out.println("error "+this.removedNotIndexedItemCounter);*/
		
		
		if (remBucket.getCount()>0){
			// find all buckets in this QTree whose MBBs OVERLAP remBucket
			buckList = this.getAllBuckets();
			responsibleBuckList = new Vector<QTreeBucket>();
			
			for (int i=0;i<buckList.size();i++){
				// if i'th entry contains remBucket's MBB -- put that entry into the list of responsible buckets
				if (buckList.get(i).overlaps(remBucket)){
					responsibleBuckList.add(buckList.get(i));
				}
			}
			
			switch (this.currentRemoveStrategy) {
			case BUCKET_MERGING:
				// remBucket after calling this method has a counter representing the number of non-deleted records
				removeDataItemApplyingBucketMerging(remBucket,responsibleBuckList); // true encodes that responsibleBuckets contains only Buckets that completely enclose remBucket
				break;
			case USING_DISTANCETOCENTER:
				removeDataItemApplyingUsingDistanceToNodeCenter(remBucket,responsibleBuckList);
				break;
			case USING_OVERLAP:
				removeDataItemApplyingUsingOverlap(remBucket, responsibleBuckList);
				break;
			default:
				removeDataItemApplyingBucketMerging(remBucket,responsibleBuckList);
				break;
			}
		}
		

		// if there are still some records to delete left (is is impossible when applying "BUCKET_MERGING" as remove strategy),s
		if ( remBucket.getCount()>0 ){
			
			// consider all other buckets to delete from as well
			if (this.currentRemoveStrategy == RemoveStrategy.USING_DISTANCETOCENTER){
				
				responsibleBuckList =  this.getAllBuckets();
				
				removeDataItemApplyingUsingDistanceToNodeCenter(remBucket,responsibleBuckList);
				
				if (remBucket.getCount() > 0 ){
					//System.err.println("ErrorCounter = "+this.removedNotIndexedItemCounter);
					this.removedNotIndexedItemCounter += remBucket.getCount();
				}
			} else if (this.currentRemoveStrategy == RemoveStrategy.USING_OVERLAP){
				
				responsibleBuckList =  this.getAllBuckets();
				
				removeDataItemApplyingUsingDistanceToNodeCenter(remBucket,responsibleBuckList);
				
				if (remBucket.getCount() > 0 ){
					//System.err.println("ErrorCounter = "+this.removedNotIndexedItemCounter);
					this.removedNotIndexedItemCounter += remBucket.getCount();
				}
			}
		}
		
		// assert checkNoZeroCountBucket() : "Bucket with Zero Length found !";
	}
	
	
	/**
	 * Checks whether there are Buckets with Zero Count
	 * @return True if there are no Buckets with Zero Count
	 */
	protected boolean checkNoZeroCountBucket() {
		Vector <QTreeBucket> buckList = this.getAllBuckets();
		for (QTreeBucket b : buckList) {
			if (b.getCount() < 0.5) {
				System.out.println(b);
				return false;
			}
		}
		return true;
	}
	
	/* (non-Javadoc)
	 * @see smurfpdms.index.BucketBasedLocalIndex#getAllBuckets(boolean)
	 */
	@Override
	public Vector<Bucket> getAllBuckets(boolean clone) {
		Vector<QTreeBucket> bucklist = this.getAllBuckets();
		Vector<Bucket> retbucklist = new Vector<Bucket>();
		//
		for (QTreeBucket qtb : bucklist) {
			if (clone) {
				retbucklist.add(qtb.clone());
			} else {
				retbucklist.add(qtb);
			}
		}
		return retbucklist;
	}


	/* (non-Javadoc)
	 * @see smurfpdms.index.LocalIndex#createEmptyLocalIndexForNeighbor(smurfpdms.peer.Neighbor)
	 */
	//@Override
	//public LocalIndex createEmptyLocalIndexForNeighbor(Neighbor neighbor) {
	public LocalIndex createEmptyLocalIndexForNeighbor() {
		/*return new QTree(
			this.nmbOfDimensions, this.maxFanout, this.maxBuckets, 
			this.getDimSpecMin(), this.getDimSpecMax(), neighbor.getPeerID(), neighbor.getNeighborID()
		);*/
		return new QTree(
				this.nmbOfDimensions, this.maxFanout, this.maxBuckets, 
				this.getDimSpecMin(), this.getDimSpecMax(), "0", "1", this.storeDetailedCounts
			);
	}


	/* (non-Javadoc)
	 * @see smurfpdms.index.BucketBasedLocalIndex#insertBucket(smurfpdms.update.UpdateBucket)
	 */
	@Override
	public void insertBucket(UpdateBucket bucket, boolean insertIntoExistingBucketIfPossible) throws Exception {
		// Insert the given Bucket as a new Bucket with 
		// a new Unique Local Bucket ID
		// The given ID would be ignored
		//System.out.println("Insert Bucket");
		QTreeBucket nb;
		if (!this.storeDetailedCounts){
			nb = new QTreeBucket(this,null,bucket.getLowerBoundaries(),bucket.getUpperBoundaries(),
	              bucket.getCount(),this.getPeerID(),this.getNeighborID(),
	              this.getQTreeUniqueBucketID(), bucket.getSourceIDs());
		} else {
			nb = new QTreeBucket(this,null,bucket.getLowerBoundaries(),bucket.getUpperBoundaries(),
		          bucket.getCount(),this.getPeerID(),this.getNeighborID(),
		          this.getQTreeUniqueBucketID(), bucket.getSourceIDMap());
		}
		
		
		// For Evaluation
		if (Parameters.storeDataPointsInBuckets) {
			nb.addPoints(bucket.getContainedPoints());
		}
		
		this.insertDataItem(nb,false,insertIntoExistingBucketIfPossible);
		
		if (insertIntoExistingBucketIfPossible) assert checkNoZeroCountBucket() : "Bucket with Zero Length found !";
	}


	/* (non-Javadoc)
	 * @see smurfpdms.index.BucketBasedLocalIndex#removeBucket(smurfpdms.update.UpdateBucket)
	 */
	@Override
	public void removeBucket(UpdateBucket bucket) throws Exception{
		// first test if the given bucket is in the index
		// Create a Dummy QTreeBucket from the UpdateBucket
		System.out.println("Remove Bucket");
		QTreeBucket remBucket = new QTreeBucket(this,null,bucket.getLowerBoundaries(),
												bucket.getUpperBoundaries(),
												bucket.getCount(),
												this.getPeerID(),
												this.getNeighborID(),
												0, new HashSet<String>());
		// TODO: Hack for SemanticQTree project
		// For Evaluation
		if (Parameters.storeDataPointsInBuckets) {
			remBucket.addPoints(bucket.getContainedPoints());
		}
		
		try {
			// Now call the "regular" removeDataItem Method with the dummy
			removeDataItem(remBucket);
		} catch (Exception e) {
		    // If this fails 
			System.err.println(e.getMessage());
			System.err.println("Applying Heuristics !!");
			this.rootNode.removeBucketwithHeuristics(remBucket);
		}
		assert checkNoZeroCountBucket() : "Bucket with Zero Length found !";
	}

	
	/* (non-Javadoc)
	 * @see smurfpdms.index.BucketBasedLocalIndex#updateBucket(smurfpdms.update.UpdateBucket)
	 */
	@Override
	public void updateBucket(UpdateBucket bucket) throws Exception {
		System.out.println("Update Bucket");
		// Semantics:
		// An update means the following:
		// Take a QTree some time ago ...
		// Then Insert and Remove some Data Items and/or Buckets
		// Now compare the resulting Tree with the old one
		// and figure out all the differences
		// Then try to "update" 
		if (bucket.getCount() > 0) {
			// Just do an regular insert
			// A new local ID would be created but if these bucket would be merged with
			// an existing then this doesn't matter
			this.insertBucket(bucket,true);
		} else { 
			QTreeBucket remBucket = new QTreeBucket(this,null,
													bucket.getLowerBoundaries(),
													bucket.getUpperBoundaries(),
													(-bucket.getCount()),this.getPeerID(),
													this.getNeighborID(),
													this.getQTreeUniqueBucketID(),
													new HashSet<String>());
			// For Evaluation
			if (Parameters.storeDataPointsInBuckets) {
				remBucket.addPoints(bucket.getContainedPoints());
			}
			
			// If the updCount is negative we've got a problem
			// The boundaries could have changed too
			// Now we have to figure out on which bucket we have to reduce the count
			// Also we have to take into consideration that removing a bucket within
			// an update should be the very latest consequence ...
			this.rootNode.removeBucketwithHeuristics(remBucket);
		}	
		assert checkNoZeroCountBucket() : 
			   "Bucket with Zero Length found ! ";
	}

	/* (non-Javadoc)
	 * @see smurfpdms.index.BucketBasedLocalIndex#getNmbOfBuckets()
	 */
	@Override
	public int getNmbOfBuckets() {
		Vector<QTreeBucket> buckList = this.getAllBuckets();
		return buckList.size();
	}

	/* (non-Javadoc)
	 * @see smurfpdms.index.BucketBasedLocalIndex#getNmbOfIndexedItems()
	 */
	@Override
	public int getNmbOfIndexedItems() {
		int itemCount=0;
		Vector<QTreeBucket> buckList = this.getAllBuckets();
	    for (QTreeBucket b : buckList){
	    	itemCount += b.getCount();
	    }
	    return itemCount;
	}
	
	/**
	 * 
	 * @return The Number of Buckets that are Merged together in One Step
	 */
	public int getMergeAtOnce(){
		return this.mergeAtOnce;
	}

	/* (non-Javadoc)
	 * @see smurfpdms.index.BucketBasedLocalIndex#hasFixedBucketBoundaries()
	 */
	@Override
	public boolean hasFixedBucketBoundaries() {
		return false;
	}

	/* (non-Javadoc)
	 * @see smurfpdms.index.BucketBasedLocalIndex#getAllBucketsInQuerySpace(smurfpdms.index.QuerySpace)
	 */
	@Override
	public Vector<IntersectionInformation> getAllBucketsInQuerySpace(QuerySpace qs) {
		Vector<IntersectionInformation> result =  new Vector<IntersectionInformation>();
		Vector<QTreeBucket> buckList = this.getAllBuckets();
		
		// the simple variant: no constraints over the query space
		if (qs.hasNoRestrictions()) {
			for (int i = 0; i < buckList.size(); i++) {
				result.add(new IntersectionInformation(buckList.get(i), buckList.get(i), 1.0));
			}

		} else { // otherwise we must iterate over each bucket and check whether it intersects 
                 // with the query space
			for (QTreeBucket currentBucket : buckList){
				Space intersection = currentBucket.intersect(qs);
				if (intersection != null) {
					// add to result list
					result.add(computeIntersectionInformation(currentBucket, qs));
				}
				else {
					// do nothing
				}
			}
		}
		// now return the buckets (if any)
		return result;
	}
	
	/* (non-Javadoc)
	 * @see smurfpdms.index.LocalIndex#buildJSubTree(javax.swing.tree.DefaultMutableTreeNode)
	 */
	@Override
	public void buildJSubTree(DefaultMutableTreeNode parentNode){
		DefaultMutableTreeNode newNode = new DefaultMutableTreeNode(
			"indexed attributes: " + this.indexOnDimensions.toString()
		);
		parentNode.add(newNode);

		newNode = new DefaultMutableTreeNode("max. fanout: " + this.maxFanout);
		parentNode.add(newNode);

		newNode = new DefaultMutableTreeNode("max. number of buckets: " + this.maxBuckets);
		parentNode.add(newNode);

		newNode = new DefaultMutableTreeNode("used number of buckets: " + this.currNmbOfBuckets);
		parentNode.add(newNode);

		newNode = new DefaultMutableTreeNode("mergePenaltyFunction: " + this.mergePenaltyFunction.getID());
		parentNode.add(newNode);

		newNode = new DefaultMutableTreeNode("groupPenaltyFunction: " + this.groupPenaltyFunction.getID());
		parentNode.add(newNode);
		
		newNode = new DefaultMutableTreeNode("Nodes");
		parentNode.add(newNode);

		this.rootNode.buildJSubTree(newNode);
	}
} // END CLASS QTree