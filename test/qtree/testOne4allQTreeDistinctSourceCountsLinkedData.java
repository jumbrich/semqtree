/**
 * 
 */
package qtree;

import ie.deri.urq.wods.hashing.QTreeHashing;
import ie.deri.urq.wods.hashing.us.PrefixTreeHashing;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import junit.framework.TestCase;

import org.semanticweb.wods.indexer.InsertCallback;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars2.index.disk.QuadStringScanIterator;
import org.semanticweb.yars2.index.disk.block.NodeBlockInputStream;

import de.ilmenau.datasum.index.Bucket;
import de.ilmenau.datasum.index.OnDiskOne4AllQTreeIndex;
import de.ilmenau.datasum.index.qtree.QTreeBucket;
import de.ilmenau.datasum.index.qtree.QTreeNode;
import de.ilmenau.datasum.index.update.UpdateBucket;

/**
 * @author khose
 *
 */
public class testOne4allQTreeDistinctSourceCountsLinkedData extends TestCase {
	
	
	public void testStatementInsertion() throws Exception {
		
		int dimMin = 0;
		int dimMax = 10000; // upper limit...!!
		int maxBuckets = 25;
		int maxFanout = 20;
		
		boolean testInsertion = false; // sources per count: test that after each insertion, one bucket changed its total count
		
		QTreeHashing hasher = new PrefixTreeHashing();
//		QTreeHashing hasher = new MarioHashing();
//		QTreeHashing hasher = new CheatingSimpleHashing(new int[]{21,695,-1318}, new int[]{1836,1695,1836}, 32, 252);
//		QTreeHashing hasher = new SimpleHashing(32, 252);
	    
		
		// read/create index storing only one count value per bucket
	    File outputDir = new File("output/oldCount");
		File inputIDX = new File("input/spoc.idx");
	    //OnDiskOne4AllQTreeIndex indexOldImplementation = new OnDiskOne4AllQTreeIndex(hasher,1500,20,new int[]{dimMin,dimMin,dimMin}, new int[] {dimMax,dimMax,dimMax});
	    OnDiskOne4AllQTreeIndex indexOverallSourcesCount = new OnDiskOne4AllQTreeIndex(hasher,maxBuckets,maxFanout,new int[]{dimMin,dimMin,dimMin}, new int[] {dimMax,dimMax,dimMax},false);
	    
	    
	    double overallInsertedSourcesCount = 0.0;
    	File qtreeFile = new File(outputDir,indexOverallSourcesCount.getFileName());
    	if(qtreeFile.exists()){
    	    System.out.println("[DEBUG] Found serialised version of a qtree in the output dir "+outputDir);
    	    indexOverallSourcesCount.loadQTreeIndex(outputDir);
    	} else {
    	    //System.err.println("[DEBUG] Could not found serialised version of a qtree in the output dir "+outputDir);
    	    //System.err.println("[DEBUG] Building new QTree from statements in "+ inputIDX);
    	    
    	    InsertCallback c = new InsertCallback(indexOverallSourcesCount);
		    NodeBlockInputStream nbis = new NodeBlockInputStream(inputIDX.getAbsolutePath());
		    QuadStringScanIterator iter = new QuadStringScanIterator(nbis);
		    int count=0;
		    while(iter.hasNext()){
		    	//System.out.print(".");
		    	c.processStatement(iter.next());
		    	count++;
		    	if(count%10000==0)
		    		System.out.println("DEBUG inserted "+count+" stmts");
		    }
		    System.out.println("inserted "+c.insertedStatments()+" stmts");
		    
		    for (QTreeBucket curr: indexOverallSourcesCount.getQTree().getAllBuckets()){
		    	overallInsertedSourcesCount += curr.getCount();
		    }
		    
		    if (c.insertedStatments() != overallInsertedSourcesCount){
		    	System.err.println("QTree-Buckets (overall) do not represent all inserted statements, inserted="+c.insertedStatments()+", represented="+overallInsertedSourcesCount);
		    }
		    
		    System.out.println("[DEBUG] Serialising QTree from statements to "+ outputDir);
		    indexOverallSourcesCount.serialiseQTree(outputDir);
    	}
    	System.out.println(indexOverallSourcesCount.getQTree().getStateString());
    	
    	
    	
    	boolean recreateNewIndex = true;
    	// new index with count per sourceID in each bucket
	    OnDiskOne4AllQTreeIndex indexSingleSourceCount = new OnDiskOne4AllQTreeIndex(hasher,maxBuckets,maxFanout,new int[]{dimMin,dimMin,dimMin}, new int[] {dimMax,dimMax,dimMax},true);
	    
	    double indexSingleInsertedCount = 0.0;
	    File newOutputDir = new File("output");
	    File newQtreeFile = new File(newOutputDir,indexSingleSourceCount.getFileName());
    	if(!recreateNewIndex && newQtreeFile.exists()){
    	    System.out.println("[DEBUG] Found serialised version of a qtree in the output dir "+newOutputDir);
    	    indexSingleSourceCount.loadQTreeIndex(newOutputDir);
    	} else {
       	    InsertCallback c = new InsertCallback(indexSingleSourceCount);
    	    NodeBlockInputStream nbis = new NodeBlockInputStream(inputIDX.getAbsolutePath());
    	    QuadStringScanIterator iter = new QuadStringScanIterator(nbis);
    	    
    	    // insert statements
    	    int count=0;
    	    while(iter.hasNext()){
    	    	
    	    	String oldIndexState = "";
    	    	HashMap<Long,Double> oldControlMap = new HashMap<Long,Double>();    	    	
        	    if (testInsertion){
        	    	indexSingleSourceCount._sourcesQTree.getStateString();
	        	    for (QTreeBucket buck: indexSingleSourceCount._sourcesQTree.getAllBuckets()){
	        	    	oldControlMap.put(buck.getLocalBucketID(),buck.getTotalCount());
	        	    }
        	    }
    	    	
    	    	Node[] quad = iter.next();
    	    	c.processStatement(quad);
    	    	count++;
    	    	if(count%10000==0)
    	    		System.out.println("DEBUG inserted "+count+" stmts");
    	    	
        	    HashMap<Long,Double> newControlMap = new HashMap<Long,Double>();
        	    if (testInsertion){
            	    for (QTreeBucket buck: indexSingleSourceCount._sourcesQTree.getAllBuckets()){
            	    	newControlMap.put(buck.getLocalBucketID(),buck.getTotalCount());
            	    }
        	    }
        	    
        	    if (testInsertion){
            	    boolean passed = false;
            	    for (Entry<Long,Double> ent: newControlMap.entrySet()){
            	    	Double oldValue = oldControlMap.get(ent.getKey());
            	    	if (oldValue == null || oldValue<ent.getValue()){
            	    		passed = true;
            	    	} 
            	    }
            	    if (!passed){
            	    	String[] converted = {quad[0].toN3(), quad[1].toN3(), quad[2].toN3()};
            	    	int[] coord = hasher.getHashCoordinates(converted, new int[]{dimMin,dimMin,dimMin}, new int[] {dimMax,dimMax,dimMax});
        	    		System.err.println("insertion error: "+Arrays.toString(coord));
        	    		System.err.println("old index: "+oldIndexState);
        	    		System.err.println("new index: "+indexSingleSourceCount._sourcesQTree.getStateString());
            	    }
        	    }
    	    }
    	    System.out.println("inserted "+c.insertedStatments()+" stmts");
		    
    	    for (QTreeBucket curr: indexSingleSourceCount._sourcesQTree.getAllBuckets()){
    	    	indexSingleInsertedCount += curr.getTotalCount();
    	    }
    	    
		    if (c.insertedStatments() != indexSingleInsertedCount){
		    	System.err.println("QTree-Buckets (SingleSource) do not represent all inserted statements, inserted="+c.insertedStatments()+", represented="+indexSingleInsertedCount);
		    }
		    
		    System.out.println("[DEBUG] Serialising QTree from statements to " + newOutputDir);
		    indexSingleSourceCount.serialiseQTree(newOutputDir);
    	}
	    
	    System.out.println(indexSingleSourceCount.getQTree().getStateString());
    	
	    
	    // test if all statements have been inserted into the buckets (inner nodes are known to have incorrect count values)
	    /*assert(overallInsertedSourcesCount==indexSingleInsertedCount);
	    if (overallInsertedSourcesCount!=indexSingleInsertedCount){
	    	System.err.println("The two implementations contain different numbers of statements " +
	    			"after creation overall="+overallInsertedSourcesCount+", perSource="+indexSingleInsertedCount);
	    }*/
	}
	
	public void testBucketInsertion() throws Exception {
		
		QTreeHashing hasher = new PrefixTreeHashing();
//		QTreeHashing hasher = new MarioHashing();
//		QTreeHashing hasher = new CheatingSimpleHashing(new int[]{21,695,-1318}, new int[]{1836,1695,1836}, 32, 252);
//		QTreeHashing hasher = new SimpleHashing(32, 252);
	    
		
		// start with creating a bucket for the overall source counting variant
	    File outputDir = new File("output/oldCount");
	    testBucketInsertion(false, outputDir, hasher);
	    System.out.println("inserting Buckets for sourceID overall count: passed");
	    
	    // proceed with the variant storing counts for distinct sourceIDs
	    outputDir = new File("output");
	    testBucketInsertion(true, outputDir, hasher);
	    System.out.println("inserting Buckets for count per sourceID: passed");
	}
	
	
	public void testBucketInsertion(boolean storeDetailedCounts, 
			File outputDir, QTreeHashing hasher) throws Exception {
		
		int dimMin = 0;
		int dimMax = 10000; // upper limit...!!
		int maxBuckets = 25;
		int maxFanout = 20;
	
		OnDiskOne4AllQTreeIndex index = new OnDiskOne4AllQTreeIndex(hasher,maxBuckets,maxFanout,new int[]{dimMin,dimMin,dimMin}, new int[] {dimMax,dimMax,dimMax},storeDetailedCounts);
	    
	    // testing overall bucket count first
    	File qtreeFile = new File(outputDir,index.getFileName());
    	if(qtreeFile.exists()){
    	    System.out.println("[DEBUG] Loading version of a qtree in the output dir "+outputDir);
    	    index.loadQTreeIndex(outputDir);
    	} else {
    		System.out.println("Serialised input QTree not found");
    	}
    	
	    // test inserting these buckets into a new QTree
	    
	    OnDiskOne4AllQTreeIndex newIndex = new OnDiskOne4AllQTreeIndex(hasher,storeDetailedCounts);
	    
	    for (Bucket currBucket : index._sourcesQTree.getAllBuckets()){
	    	UpdateBucket insertBucket = 
	    		new UpdateBucket(currBucket.getLowerBoundaries(),
	    				currBucket.getUpperBoundaries(),
	    				currBucket.getCount(),
	    				currBucket.getAttributeNames(),
	    				Integer.parseInt(currBucket.getPeerID()),
	    				Integer.parseInt(currBucket.getNeighborID()),
	    				currBucket.getLocalBucketID());
	    	insertBucket.setSourceIDMap(currBucket.getSourceIDMap());
	    	
	    	//System.out.println("Insert Bucket "+insertBucket.toString());
	    	
	    	newIndex.addBucket(insertBucket, false); // allowing buckets to be merged upon insertion
	    	
			// test if sourceIDs are inserted correctly
			boolean error = false;
			for (QTreeBucket currBuck: newIndex.getQTree().getAllBuckets()){
				Iterator<String> iter = currBuck.getSourceIDs().iterator();
				while(iter.hasNext()){
					String currSourceID = iter.next();
					QTreeNode currNode = currBuck;
					while (currNode != null){
						//error = error || !currNode.getSourceIDs().contains(currSourceID); // why does this comparison not work? The string is contained here and the containment test returns false???
						boolean contained = false;
						for (String currString : currNode.getSourceIDs()){
							if (currString.equals(currSourceID)){
								contained = true;
								break;
							}
						}
						error = error || !contained;
						if (error) {
							System.err.println("currNode (LID "+currNode.getLocalBucketID()+"): "+currNode.getBoundsString());
							break;
						}
						currNode = currNode.getParent();
					}
					if (error) {
						System.err.println(", currSourceID: "+currSourceID);
						break;
					}
				}
				if (error) {
					System.err.println(", currBucket (LID "+currBuck.getLocalBucketID()+"): "+currBuck.getBoundsString());
					break;
				}
			}
			if (error) {
				//System.err.println(newIndex.getQTree().getStateString());
				//System.exit(0);
				throw new Exception();
			} else {
				//System.out.println("passed containment test");
			}
	    	
	    }
	    
	    //test if the counts of all buckets are correct
	    double sumCountIndex = 0.0;
	    for (Bucket currBucket: index.getQTree().getAllBuckets()){
	    	sumCountIndex += currBucket.getCount();
	    }
	    
	    double sumCountIndexNew = 0.0;
	    for (Bucket currBucket: newIndex.getQTree().getAllBuckets()){
	    	sumCountIndexNew += currBucket.getCount();
	    }	    
	    
	    if (sumCountIndex != sumCountIndexNew){
	    	System.err.println("Counter error! old index: "+sumCountIndex + "new index: "+sumCountIndexNew);
	    	throw new Exception();
	    }
	    
	    /*System.out.println(index._sourcesQTree.getStateString());
	    System.out.println(newIndex._sourcesQTree.getStateString());*/
	}
	
}
