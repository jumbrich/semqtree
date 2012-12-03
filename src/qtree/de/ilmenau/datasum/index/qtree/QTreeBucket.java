/**
 * 
 */
package de.ilmenau.datasum.index.qtree;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

import de.ilmenau.datasum.config.Parameters;
import de.ilmenau.datasum.exception.QTreeException;
import de.ilmenau.datasum.util.DataPoint;

/**
 * @author matz, hose
 *
 */
public class QTreeBucket extends QTreeNode {
	private static final long serialVersionUID = 1L;
	/**
	 * 
	 * @param qtree The QTree this Bucket belongs to
	 * @param parent The parent node of this Bucket
	 * @param minBounds Minimum Boundaries for each dimension
	 * @param maxBounds Maximum Boundaries for each dimension
	 * @param count The initial Count of "Elements"
	 * @param peerID The ID of the peer that maintains this bucket
	 * @param neighborID The ID of the neighboring peer whose data this bucket describes
	 * @param localBucketID The local ID of this bucket (unique only in the corresponding QTree)
	 */
	public QTreeBucket(QTree qtree, QTreeNode parent, int[] minBounds, int[] maxBounds, 
			double count, String peerID, String neighborID, long localBucketID, 
			HashSet<String> sourceIDs) {
		
		super(qtree, parent, minBounds, maxBounds, count, peerID, neighborID, localBucketID, sourceIDs);
		
		// global ID is a combination of peerID-neighborID-localBucketID 	
	}
	
	/**
	 * 
	 * @param qtree The QTree this Bucket belongs to
	 * @param parent The parent node of this Bucket
	 * @param minBounds Minimum Boundaries for each dimension
	 * @param maxBounds Maximum Boundaries for each dimension
	 * @param count The initial Count of "Elements"
	 * @param peerID The ID of the peer that maintains this bucket
	 * @param neighborID The ID of the neighboring peer whose data this bucket describes
	 * @param localBucketID The local ID of this bucket (unique only in the corresponding QTree)
	 */
	public QTreeBucket(QTree qtree, QTreeNode parent, int[] minBounds, int[] maxBounds, 
			double count, String peerID, String neighborID, long localBucketID, 
			HashMap<String,Double> sourceIDMap) {
		
		super(qtree, parent, minBounds, maxBounds, count, peerID, neighborID, localBucketID, sourceIDMap);
		
		// global ID is a combination of peerID-neighborID-localBucketID 	
	}
	
	/**
	 * 
	 * @param qtree The QTree this Bucket belongs to
	 * @param parent The parent node of this Bucket
	 * @param minBounds Minimum Boundaries for each dimension
	 * @param maxBounds Maximum Boundaries for each dimension
	 * @param count The initial Count of "Elements"
	 * @param peerID The ID of the peer that maintains this bucket
	 * @param neighborID The ID of the neighboring peer whose data this bucket describes
	 * @param localBucketID The local ID of this bucket (unique only in the corresponding QTree)
	 */
	public QTreeBucket(QTree qtree, QTreeNode parent, int[] minBounds, int[] maxBounds, 
			String peerID, String neighborID, long localBucketID, 
			HashMap<String,Double> sourceIDsCount) {
		
		super(qtree, parent, minBounds, maxBounds, peerID, neighborID, localBucketID, sourceIDsCount);
		
		// global ID is a combination of peerID-neighborID-localBucketID 	
	}
	
	
	public void insertDataItem(int [] coordinates) {
		
		// Falls ein Bucket das MostResponsibleChild dann
		// einfach eintragen
		this.count++;
		// only for evaluation store the points in the bucket!
		if (Parameters.storeDataPointsInBuckets) {
			this.addPoint(
				new DataPoint(coordinates, this.qtree.getParentNodeName(), this.qtree.getAttributeNames())
			);
		}
		return;   	
    }
 
    public QTreeBucket clone(){
    	return (QTreeBucket) super.clone();
    }
    
    protected void cloneStructure(QTree newQTree,QTreeNode newParent,HashMap assocTable){
		this.qtree = newQTree;
		this.parent = newParent;
		// Because Buckets are not in the PriorityQueue they wouldn't be added to the assocciationTable
    }
        
    
    /**
     * returns true if this bucket contains relevant data with respect to the conditions
     * encoded by <emph>attrNames</emph>, <emph>ops</emph>, and <emph>attrValues</emph>
     */
    public boolean checkDataAvail(String [] attrNames,
    							  int [] ops,
    							  double [] attrValues) throws QTreeException{

    	// if the attribute is indexed and the bucket matches the "query"
    	// then matchingDimensions is incremented
    	
    	int matchingDimensions = 0;
    	int opindex;
    	for (opindex=0;opindex<attrNames.length;opindex++){
    		// get the DimensionIndex for this atrribute
    		// compare this buckets' dimensional definition
    		// to the operation and the given comparison attribute value
    		int i=-1; 
    		boolean found=false;
    		do {
    			i++;
    			if (attrNames[opindex].equals(this.qtree.getAttributeNames().get(i))) found=true;    			
    		} while ((i < this.qtree.getAttributeNames().size()) && (!found)) ;
    		
    		if (found) { // attribute supported by index 
    			// check condition
    			// check if the condition is contradictory to the bucket's definition in the respective dimension
    			switch (ops[opindex]) { 
    			  case QTree.OP.SMALLER: {
    				  if (this.lowerBoundaries[i] < attrValues[opindex]) matchingDimensions++;
    				  break;
    			  }
    			  case QTree.OP.SMALLER_OR_EQUAL: {
    				  if (this.lowerBoundaries[i] <= attrValues[opindex]) matchingDimensions++;
    				  break;
    			  }
    			  case QTree.OP.EQUAL: {
    				  if ((this.lowerBoundaries[i] <= attrValues[opindex]) && (this.upperBoundaries[i] >= attrValues[opindex])) matchingDimensions++;
    				  break;
    			  }
    			  case QTree.OP.GREATER_OR_EQUAL: {
    				  if (this.upperBoundaries[i] >= attrValues[opindex]) matchingDimensions++;
    				  break;
    			  }
    			  case QTree.OP.GREATER: {
    				  if (this.upperBoundaries[i] > attrValues[opindex]) matchingDimensions++;
    				  break;
    			  }
    			  default: { 
    				  throw new QTreeException("Invalid ");
    			  }
    			}    			    		    
    		} else { // 
    			//matchingDimensions++;
    		}
    	}
    	return (matchingDimensions == attrNames.length);
    }
    
    /**
     * Updates the Bucket Counter by <i>updCount</i> and propagates the changes up through the tree
     * until the RootNode is reached. Note ! 
     * 
     * @param updCount The number by which the current BucketCount should be increased or decreased
     */
    public void updateCount(double updCount){
		if ((this.count + updCount) < -0.5) {
//			System.out.println("this.count = "+this.count);
//			System.out.println("updCount = "+updCount);
			throw new IllegalStateException("QTreeBucket: Not enough data items in the bucket after update!");
		}
//		System.out.println("Processing "+this);
//		System.out.println("updateCount: this.count = "+this.count);
//		System.out.println("updateCount: updCount = "+updCount);
		this.count += updCount;
//		System.out.println("updateCount(after Update): this.count = "+this.count);
		// remove if empty
		if (this.count < 0.5) {
			try {
//				System.out.println("Removing Bucket");
//				System.out.println("Before Removing: "+this.qtree.getCurrBuckets());
				this.removeDataItem(this);
//				System.out.println("After Removing: "+this.qtree.getCurrBuckets());
			} catch (QTreeException e) {
				e.printStackTrace();
			}
		} else {
			// else update the number of elements in the index
			this.parent.updateCountBottomUp(updCount);
		}
		assert this.qtree.checkNoZeroCountBucket() : "Bucket with Zero Length found !";
    }
    
    /**
     * Removes the given <i>bucket</i> which has to be fully enclosed by the current Bucket
     * --> there is no check if this condition is fufilled!!!
     * 
     * 
     *  
     * @param bucket The Bucket to remove
     * @throws QTreeException If the removeCount is too high
     */
    @SuppressWarnings("unchecked")
	protected double removeDataItem(QTreeBucket bucket) throws QTreeException{
    	//intialize error counter
    	double errDiff=0;
    	
    	// if the number of contained records after removal would be less than 0 (i.e., if more records than inserted shall be removed), 
    	// put difference into error counter and proceed with deting the bucket 
    	// (setting the counter of bucket to the counter of this bucket)
    	if ((this.count - bucket.count) < -0.5) {
    		// Maybe in future Releases: Throw Exception
    		// throw new QTreeException("QTreeBucket: Trying to Remove more Items that are indexed");
    		// for now just Increment the Failure Counter of the Index
    		errDiff = bucket.count - this.count; // Store the Wrong Item Count
    		bucket.count = this.count; // Just remove regularly
    	}
    	
    	// if there are no entries left after the removal --> delete this bucket node
    	if ((this.count - bucket.count) < 0.5) {
    		// This Bucket has to be removed
    		// don't forget to adjust parent boundaries !
			this.parent.children.remove(this);
			this.qtree.decCurrBucketCount();
			
			// we can remove -bucket.count because the counter has prbably been set to this.count
			this.parent.updateCountAndBoundsBottomUp(-bucket.count); 
			
			// ### Rebalancing the Tree ###
			try {
				Vector<QTreeNode> parentChildList = (Vector<QTreeNode>) this.parent.children.clone();
				
				// We have to call tryToDropNode on each sibling Node for the folowing case
				//       N1
				//    /  |   \
				//   b1  b2   N2
				//           /  \
				//          b3   b4
				// b1 is deleted ... to rebalance the Tree we have to put b3 and b4 as children of
				// N1 and delete N2
				boolean dropped = false;
				for (QTreeNode q : parentChildList){
					if (q.tryToDropNode()){
						dropped = true;
					}
				}
				
				// After that we also have to call tryToDropNode from the parent Node
				//       N1
				//    /  |   \
				//   b1  b2   N2
				//           /  \
				//          b3   b4
				// in this case b3 is deleted and the previous Case doesn't handle this
				// By calling tryToDropNode of parent(N2) we put b4 as child to N1, what we want ...
				if (this.parent.tryToDropNode()) {
					dropped = true;
				}
				
				// If nothings could be dropped we have to update the Priority Queue(thx Katja)
				if (!dropped) {
					this.parent.updatePrioQueue();
				}
				//Note: Calling tryToDropNode doesn't hurt if theres nothing to drop
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			// remove reference to parent
			this.parent = null;
			
			this.count = 0;
			
			// We don't have to give the boundaries of the removed Node to the Parent
			// because in the parent the new bounds are recalculated with the
			// remaining childs

		// if the counter of this bucket is greater than the number of entries to remove (bucket.count) 
    	} else {
    		
    		this.count -= bucket.count;
    		
    		if (this.parent != null){
    			this.parent.updateCountBottomUp(-bucket.count);
    		}
    		
    		// only for evaluation remove the points in the bucket
    		if ((Parameters.storeDataPointsInBuckets) && (this.qtree.debugChanges)) {
    			int remPointCntBefore = this.getContainedPoints().size();
    			int remPointCnt = bucket.getContainedPoints().size();
    			this.removePoints(bucket.getContainedPoints());
    			int remPointCntAfter = this.getContainedPoints().size();
    			if ((remPointCntBefore - remPointCntAfter) < remPointCnt) {
    				System.out.println("Some Points in the removeBucket dont exist in the current Bucket !");
    				System.out.println("");
    			}
    		}
    	}
    	
    	// Do all stuff correctly first but assign that more Items should have been removed
    	/*if (errDiff > 0){
    		throw new QTreeException(Double.toString(errDiff));
    	}*/
    	return errDiff;
    }
    
    /**
     * sets the counter of this bucket to the input value
     * 
     * @param newCount
     */
    protected void setCount(double newCount) {
    	this.count = newCount;
    }
    
    
}