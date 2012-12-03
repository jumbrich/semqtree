package qtree;

import ie.deri.urq.wods.bench.JoinBenchmark;
import ie.deri.urq.wods.hashing.CheatingSimpleHashing;
import ie.deri.urq.wods.hashing.QTreeHashing;
import ie.deri.urq.wods.hashing.SimpleHashing;
import ie.deri.urq.wods.hashing.us.MarioHashing;
import ie.deri.urq.wods.hashing.us.PrefixTreeHashing;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Vector;

import junit.framework.TestCase;

import org.semanticweb.wods.indexer.IndexerManager;
import org.semanticweb.wods.indexer.InsertCallback;
import org.semanticweb.wods.indexer.OnDiskQTreeIndexerFactory;
import org.semanticweb.wods.indexer.Queue;
import org.semanticweb.wods.lodq.BGPMatcher;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.parser.ParseException;
import org.semanticweb.yars2.index.disk.QuadStringScanIterator;
import org.semanticweb.yars2.index.disk.block.NodeBlockInputStream;
import org.semanticweb.yars2.rdfxml.RDFXMLParser;

import com.sleepycat.collections.MapEntryParameter;

import de.ilmenau.datasum.index.Bucket;
import de.ilmenau.datasum.index.IntersectionInformation;
import de.ilmenau.datasum.index.OnDiskOne4AllQTreeIndex;
import de.ilmenau.datasum.index.QuerySpace;
import de.ilmenau.datasum.index.qtree.QTree;
import de.ilmenau.datasum.index.qtree.QTreeBucket;
import de.ilmenau.datasum.index.update.UpdateBucket;
import de.ilmenau.datasum.util.bigmath.Space;

public class CopyOfTestBGPJoin extends TestCase {
	public static String DATA_DIR = "input/linked-data/";
	private int DEBUG = 0;
	
	int dimMin = 0;
	int dimMax = 10000; // upper limit...!!
	
	boolean storeDetailedCounts = true;
	boolean advancedRanking = false;
	
	public void testBGP() throws Exception {
		long time = System.currentTimeMillis();
//		System.setProperty("file.encoding","utf-8");
//		System.err.println(System.getProperty("file.encoding"));
		File dir = new File(DATA_DIR);
		String[] sources = dir.list();

		// build QTree
		QTreeHashing hasher = new PrefixTreeHashing();
//		QTreeHashing hasher = new MarioHashing();
//		QTreeHashing hasher = new CheatingSimpleHashing(new int[]{21,695,-1318}, new int[]{1836,1695,1836}, 32, 252);
//		QTreeHashing hasher = new SimpleHashing(32, 252);
		
		File outputDir = new File("output");
		File inputIDX = new File("input/spoc.idx");
	    OnDiskOne4AllQTreeIndex index = new OnDiskOne4AllQTreeIndex(hasher,1500,20,new int[]{dimMin,dimMin,dimMin}, new int[] {dimMax,dimMax,dimMax},storeDetailedCounts);
		buildOrLoad(outputDir, index, inputIDX);
//	    String dataDir = "input/linked-data/";
//	    Queue queue = new Queue(new File(dataDir));
//	    IndexerManager manager = new IndexerManager(index, 1, queue, new OnDiskQTreeIndexerFactory());
//	    manager.runIndexer();

		System.out.println("QTree: "+index.getQTree().getAllBuckets().size()+" buckets");
//		System.out.println(index.getQTree().getStateString());
//		System.exit(1);
//		index.getQTree().printme();
		JoinBenchmark joinB = new  JoinBenchmark(outputDir, index, inputIDX, "idx", "test");
		for (int i = 0; i < SampleQueries.QUERIES.length; i++) {			
			Node[][] q = SampleQueries.QUERIES[i];
			int[][] join = SampleQueries.QJ[i];
			joinB.benchmark(q, join);
			
//		for (int i = 0; i < SampleQueries.QUERIESSLOW.length; i++) {			
//			Node[][] q = SampleQueries.QUERIESSLOW[i];
//			int[][] join = SampleQueries.QJS[i];
			
			// first, evaluate on QTree
			Vector<IntersectionInformation> leftResult = index.getQTree().getAllBucketsInQuerySpace(index.getQuerySpace(q[0],false));
			try{
				QTree currResult = buildJoinQtree(leftResult,index.getQTree().storeDetailedCounts(),0);

				// do the actual BGP processing
				Map<Node[],String> current = evaluateBgp(sources, q[0]);
				System.out.println("Qtree says: "+countSources(leftResult,currResult.storeDetailedCounts())+" relevant sources");

				for (int j = 1; j < q.length; j++) {
					// first, evaluate on QTree
					Vector<IntersectionInformation> rightResult = index.getQTree().getAllBucketsInQuerySpace(index.getQuerySpace(q[j],false));
					if (0 == rightResult.size()) currResult = null;

					// do the actual BGP processing
					Map<Node[],String> gpresults = evaluateBgp(sources, q[j]);		
					System.out.println("Qtree says: "+countSources(rightResult,index.getQTree().storeDetailedCounts())+" relevant sources");

					System.out.println("joining "+current.size()+" triples with "+gpresults.size());
					if (null != currResult) currResult = computeQTreeJoin(currResult, join[j-1][0], buildJoinQtree(rightResult,index.getQTree().storeDetailedCounts(),j), join[j-1][1], j);

					current = computeJoin(current, join[j-1][0], gpresults, join[j-1][1]);
					System.out.println("result: "+current.size()+" triples");
				}
				
				Set<String> relevantSources = new HashSet<String>();
				if (null != currResult) {
					for (QTreeBucket b : currResult.getAllBuckets()) {
						if (currResult.storeDetailedCounts()) {
							for (String s : b.getSourceIDMap().keySet()) {
								relevantSources.add(s.substring(s.indexOf('|')+1));
							}
						}
						else relevantSources.addAll(b.getSourceIDs());
					}
				}

				System.out.println("relevant "+relevantSources.size()+" sources according to QTree:");
				//			for (String s : relevantSources) {
				//				System.out.println(s);
				//			}

				if (0 == current.size()) System.out.println("Empty join result.");
				Set<String> resSources = new HashSet<String>();
				for (Map.Entry<Node[],String> nx : current.entrySet()) {
					//				for (Node n : nx.getKey()) System.out.print(n.toN3()+" ");
					//				System.out.println();
					resSources.add(nx.getValue());
				}
				Set<String> allResSources = new HashSet<String>();
				for (String s : resSources) {
					StringTokenizer tok = new StringTokenizer(s,"|");
					while (tok.hasMoreTokens()) allResSources.add(URLDecoder.decode(tok.nextToken(), "utf-8"));
				}
				System.out.println("Actual "+allResSources.size()+" relevant sources:");
//				System.out.println("Actual "+resSources.size()+" relevant sources:");
				//			for (String s : resSources) {
				//				System.out.println(s);
				////				System.out.println(s+" -- "+(currRelevantSources.containsKey(URLDecoder.decode(s, "utf-8"))? "ok" : "missing!"));
				//			}
				
				TreeMap<Double,String> ranked = rankSources(currResult, join.length+1);
				Vector<String> rankOrderedSources = getRankOrderedSources(ranked, join.length+1);
				// the k values for evaluating top-k
				int[] topK = new int[]{10,50,100};
				// to store the percentages of each top-k result set
				double[] topKPerc = new double[topK.length];
				// handle each of the k values
				for (int k=0; k<topK.length; ++k) {
//					topKPerc[k] = computeTopKPercentage(ranked, topK[k], current);
					topKPerc[k] = computeTopKPercentage(rankOrderedSources, topK[k], current);
				}
				
				// compare the QTree ranks with the actual ranks
				Vector<Integer> positions = new Vector<Integer>();
				double avgErr = 0.0;
//				avgErr = getRankError(ranked, current, positions);
				avgErr = getRankError(rankOrderedSources, current, positions);
				
				// check at which k we will achieve 100% of the result (i.e., contain all contributing sources)
//				int maxK = getMaxK(ranked, allResSources, positions);
				
//				int maxK = advancedRanking? positions.lastElement() : positions.firstElement();
				int maxK = positions.lastElement();
				
				System.out.println("all actually relevant sources in QTree's top-"+maxK+" (avg error: "+avgErr+", positions: "+positions+")");
				for (int k=0; k<topK.length; ++k) {
					System.out.println("\ttop-"+topK[k]+" contains: "+topKPerc[k]+" of result");
				}
			}catch(Exception e){
				e.printStackTrace();
			}
			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
			
		}
		
		long time1 = System.currentTimeMillis();

		System.err.println("time elapsed: " + (time1-time) + " ms");
	}
	
	private int countSources(Vector<IntersectionInformation> v, boolean storeDetailedCounts) {
		Set<String> s = new HashSet<String>();
		for (IntersectionInformation i : v) {
			if (!storeDetailedCounts) s.addAll(i.getCorrespondingBucket().getSourceIDs());
			else s.addAll(i.getCorrespondingBucket().getSourceIDMap().keySet());
		}
		return s.size();
	}
	
	/**
	 * builds a QTree on the basis of some IntersectionInformation
	 * 
	 * @param spaces
	 * @return
	 */
	private QTree buildJoinQtree(Vector<IntersectionInformation> spaces, boolean storeDetailedCounts, int joinLevel) throws Exception {
		if (0 == spaces.size()) return null;

		int currDim = spaces.firstElement().getCorrespondingBucket().getLowerBoundaries().length;
		int[] dimSpecMin = new int[currDim];
		int[] dimSpecMax = new int[currDim];

		// prepare the QTree
		// TODO: should be made dynamic (maxF, maxB)
		for (int i=0; i<currDim; ++i) {
			dimSpecMin[i] = dimMin;
			dimSpecMax[i] = dimMax;
		}
		
    	if (2 < DEBUG) {
//	    	if (1.0 > interSec.getIntersectionRatio()) System.err.println("found one! ratio: "+interSec.getIntersectionRatio());
			System.out.println("building qtree on input:");
			for (IntersectionInformation i : spaces) {
				if (!storeDetailedCounts) System.out.println("\t"+i+", sources: "+i.getCorrespondingBucket().getSourceIDs().size());
				else System.out.println("\t"+i+", sources: "+i.getCorrespondingBucket().getSourceIDMap().size()+", join rank count: "+bucketCount(i.getCorrespondingBucket()));
//				else System.out.println("\t"+i.toString()+", sources: "+i.getCorrespondingBucket().getSourceIDMap());
			}
    	}
	    Vector<UpdateBucket> newBuckets = new Vector<UpdateBucket>();
		// insert the spaces from left into a temporary QTree in order to determine the overlap in the join dimension
	    for (IntersectionInformation interSec : spaces) {
	    	Bucket currBucket = interSec.getCorrespondingBucket();
	    	Space currSpace = interSec.getIntersection();
	    	// TODO: choose the boundaries of the corr. bucket or the intersected space?!?
    		UpdateBucket insertBucket = 
    			new UpdateBucket(currSpace.getLowerBoundaries(),
    					currSpace.getUpperBoundaries(),
    					currBucket.getCount()*interSec.getIntersectionRatio(),
    					currBucket.getAttributeNames(),
    					Integer.parseInt(currBucket.getPeerID()),
    					Integer.parseInt(currBucket.getNeighborID()),
    					currBucket.getLocalBucketID());
	    	if (storeDetailedCounts) {
				double bucketCnt = 0.0;
	    		HashMap<String,Double> sourceIDMap = new HashMap<String, Double>();
	    		for (Map.Entry<String,Double> e : currBucket.getSourceIDMap().entrySet()) {
	    			double newCnt = e.getValue()*interSec.getIntersectionRatio();
	    			bucketCnt += newCnt;
	    			// add the join level to the source name - thus, we maintain counts for the different levels separately
	    			sourceIDMap.put((joinLevel+1)+"|"+e.getKey(),newCnt);
	    		}
				assert 0.001 > Math.abs(bucketCnt - bucketCount(currBucket)*interSec.getIntersectionRatio()) : bucketCnt+" != "+bucketCount(currBucket)*interSec.getIntersectionRatio();
	    		insertBucket.setSourceIDMap(sourceIDMap);
	    	}
	    	else {
	    		insertBucket.setSourceIDs(currBucket.getSourceIDs());
	    	}
	    	newBuckets.add(insertBucket);
	    }

	    if (0 < DEBUG) System.out.println("buildQtree(): new buckets: "+newBuckets.size());
	    // we set the maximal bucket number so that no merging should occur (add 2 "safety" buckets)
	    QTree result = new QTree(currDim,20,newBuckets.size()+2,dimSpecMin,dimSpecMax,"0","1",storeDetailedCounts);
	    for (UpdateBucket b : newBuckets) result.insertBucket(b,false);
	    if (result.getAllBuckets().size() != newBuckets.size()) {
	    	System.err.println("CopyOfTestBGPJoin.buildQTree: Unexpected number of buckets in QTree: "+result.getAllBuckets().size()+" (expected: "+newBuckets.size()+")");
	    	System.exit(1);
	    }
	    return result;
	}
	
	// TODO: actually, we don't need a QTree on the right - could be a Vector<IntersectionInformation> as well
	public QTree computeQTreeJoin(QTree left, int lpos, QTree right, int rpos, int joinLevel) throws Exception {
		assert left.storeDetailedCounts() == right.storeDetailedCounts();
		// build intermediate result QTree
//		QTreeHashing hasher = new PrefixTreeHashing();
//	    OnDiskOne4AllQTreeIndex result = new OnDiskOne4AllQTreeIndex(hasher);

		if (1 < DEBUG) {
			System.out.println("left qtree join input:");
			for (QTreeBucket b : left.getAllBuckets()) {
				if (!left.storeDetailedCounts()) System.out.println("\t"+b+", sources: "+b.getSourceIDs().size());
				else System.out.println("\t"+b+", sources: "+b.getSourceIDMap().size()+", join rank count: "+bucketCount(b));
			}
			System.out.println("right qtree join input:");
			for (QTreeBucket b : right.getAllBuckets()) {
				if (!right.storeDetailedCounts()) System.out.println("\t"+b+", sources: "+b.getSourceIDs().size());
				else System.out.println("\t"+b+", sources: "+b.getSourceIDMap().size()+", join rank count: "+bucketCount(b));
			}
		}
		
		int currDim = left.getNmbOfDimensions();
		int newDim = currDim+3;
    	int[] dimSpecMin = new int[newDim];
    	int[] dimSpecMax = new int[newDim];
		// TODO: should be made dynamic (maxF, maxB)
		for (int i=0; i<newDim; ++i) {
			dimSpecMin[i] = dimMin;
			dimSpecMax[i] = dimMax;
		}
	    Vector<UpdateBucket> newBuckets = new Vector<UpdateBucket>();
	    // determine the actual overlap with all intersection spaces from right
	    for (QTreeBucket currBucket : right.getAllBuckets()) {
	    	//System.out.println("\t currBucket right: "+currBucket);
	    	int[] lowerBoundaries = new int[currDim];
	    	int[] upperBoundaries = new int[currDim];
	    	for (int i=0; i<lowerBoundaries.length; ++i) {
	    		// only restrict the join dimension
	    		if (i == lpos) {
	    			// use the boundaries of the right tree to limit the buckets of the left tree
	    			// should be the same vice versa
	    			lowerBoundaries[i] = currBucket.getLowerBoundaries()[rpos];
	    			upperBoundaries[i] = currBucket.getUpperBoundaries()[rpos];
	    		}
	    		else {
	    			// all other dimensions are united without restrictions
	    			lowerBoundaries[i] = dimMin;
	    			upperBoundaries[i] = dimMax;
	    		}
	    	}
	    	QuerySpace querySpace = new QuerySpace(lowerBoundaries, upperBoundaries, false);
	    	//System.out.println("\t \t querySpace: "+querySpace.toString());
	    	// TODO: does this handle redundant buckets accordingly?
	    	Vector<IntersectionInformation> overlap = left.getAllBucketsInQuerySpace(querySpace);
	    	for (IntersectionInformation info : overlap) {
	    		//System.out.println("\t \t overlap: "+info);
	    		lowerBoundaries = new int[newDim];
	    		upperBoundaries = new int[newDim];
	    		// set the first dimensions from the overlap of the left side
	    		for (int i=0; i<currDim; ++i) {
	    			lowerBoundaries[i] = info.getIntersection().getLowerBoundaries()[i];
	    			upperBoundaries[i] = info.getIntersection().getUpperBoundaries()[i];
	    		}
	    		// set the new dimensions from the right side
	    		for (int i=currDim; i<newDim; ++i) {
	    			// the join dimensions are equal
	    			if (i == currDim+rpos) {
	    				// in the join dimension we can just copy the values from the left side
		    			lowerBoundaries[i] = lowerBoundaries[lpos];
		    			upperBoundaries[i] = upperBoundaries[lpos];
	    			}
	    			else {
	    				lowerBoundaries[i] = currBucket.getLowerBoundaries()[i-currDim];
	    				upperBoundaries[i] = currBucket.getUpperBoundaries()[i-currDim];
	    			}
	    		}
	    		// determine the intersection ratio for the right side
	    		// the right side is always three-dimensional
		    	int[] rightLowerBoundaries = new int[3];
		    	int[] rightUpperBoundaries = new int[3];
		    	for (int i=0; i<rightLowerBoundaries.length; ++i) {
		    		// only restrict the join dimension
		    		if (i == rpos) {
		    			// use the boundaries of the current intersection to limit the buckets of the right tree
		    			// should be the same vice versa
		    			rightLowerBoundaries[i] = info.getIntersection().getLowerBoundaries()[lpos];
		    			rightUpperBoundaries[i] = info.getIntersection().getUpperBoundaries()[lpos];
		    		}
		    		else {
		    			// all other dimensions are united without restrictions
		    			rightLowerBoundaries[i] = dimMin;
		    			rightUpperBoundaries[i] = dimMax;
		    		}
		    	}
		    	querySpace = new QuerySpace(rightLowerBoundaries, rightUpperBoundaries, false);
		    	//System.out.println("\t \t querySpace: "+querySpace.toString());
				IntersectionInformation rightOverlap = right.computeIntersectionInformation(currBucket,querySpace);
				if (2 < DEBUG) {
//					System.out.println("new count: "+bucketCount(info.getCorrespondingBucket())+"*"+info.getIntersectionRatio()+" * "+bucketCount(currBucket)+"*"+rightOverlap.getIntersectionRatio());
					System.out.println("new count: "+info.getCorrespondingBucket().getCount()+"*"+info.getIntersectionRatio()+" * "+currBucket.getCount()+"*"+rightOverlap.getIntersectionRatio());
				}
	    		
	    		Vector<String> attributeNames = null;
//	    		Vector<String> attributeNames = new Vector<String>();
//	    		attributeNames.addAll(info.getCorrespondingBucket().getAttributeNames());
//	    		attributeNames.addAll(currBucket.getAttributeNames());
	    		// TODO: neuer boolean parameter -> false damit merge verhindert wird
		    	UpdateBucket insertBucket = 
		    		new UpdateBucket(lowerBoundaries,
		    				upperBoundaries,
		    				// it's a cross product between left and right sides
		    				info.getCorrespondingBucket().getCount()*info.getIntersectionRatio() * currBucket.getCount()*rightOverlap.getIntersectionRatio(),
		    				attributeNames,
		    				// the next three are irrelevant
		    				Integer.parseInt(currBucket.getPeerID()),
		    				Integer.parseInt(currBucket.getNeighborID()),
		    				currBucket.getLocalBucketID());
		    	if (left.storeDetailedCounts()) {
					double bucketCnt = 0.0;
		    		HashMap<String,Double> sourceIDMap = new HashMap<String, Double>();
		    		info.getCorrespondingBucket().getSourceIDMap();
		    		for (Map.Entry<String,Double> e : info.getCorrespondingBucket().getSourceIDMap().entrySet()) {
//		    			double newCnt = e.getValue()*info.getIntersectionRatio() * bucketCount(currBucket)*rightOverlap.getIntersectionRatio();
		    			double newCnt = e.getValue()*info.getIntersectionRatio() * currBucket.getCount()*rightOverlap.getIntersectionRatio();

		    			// divide by 2 because it's only one side of the join
		    			// this is one approach: 30 result triples, 3 sources -> assign 10 to each source...
//		    			newCnt /= 2.0;
		    			// ...actually, the cummulated one works better: 30 result triples, 3 sources -> assign 30 to each source (i.e., sum(assignment)=#restriples*#joinlevel)

//		    			System.out.println("newCnt: "+newCnt);
		    			bucketCnt += newCnt;
		    			sourceIDMap.put(e.getKey(),newCnt);
		    		}
		    		for (Map.Entry<String,Double> e : currBucket.getSourceIDMap().entrySet()) {
		    			// we have to divide by joinLevel, cause we accumulate (i.e., 30 triples, 3 sources -> bucketCount()=90)!
//		    			double newCnt = bucketCount(info.getCorrespondingBucket())/(joinLevel)*info.getIntersectionRatio() * e.getValue()*rightOverlap.getIntersectionRatio();
		    			double newCnt = info.getCorrespondingBucket().getCount()*info.getIntersectionRatio() * e.getValue()*rightOverlap.getIntersectionRatio();

		    			// divide by 2 because it's only one side of the join
		    			// this is one approach: 30 result triples, 3 sources -> assign 10 to each source...
//		    			newCnt /= 2.0;
		    			// ...actually, the accumulated one works better: 30 result triples, 3 sources -> assign 30 to each source (i.e., sum(assignment)=#restriples*#joinlevel)

//		    			System.out.println("newCnt: "+newCnt);
		    			bucketCnt += newCnt;
		    			// if the source already exists in the left input, it has a different join level - thus, we maintain counts for different levels separately
		    			sourceIDMap.put(e.getKey(),newCnt);
		    		}
//					assert 0.001 > Math.abs(bucketCnt - bucketCount(info.getCorrespondingBucket())/joinLevel*info.getIntersectionRatio() * bucketCount(currBucket)*rightOverlap.getIntersectionRatio() * (joinLevel+1)) : 
//						bucketCnt+" != "+bucketCount(info.getCorrespondingBucket())/joinLevel*info.getIntersectionRatio() * bucketCount(currBucket)*rightOverlap.getIntersectionRatio() * (joinLevel+1);
					assert 0.001 > Math.abs(bucketCnt/(double)(joinLevel+1) - info.getCorrespondingBucket().getCount()*info.getIntersectionRatio() * currBucket.getCount()*rightOverlap.getIntersectionRatio()) : 
						bucketCnt/(double)(joinLevel+1)+" != "+info.getCorrespondingBucket().getCount()*info.getIntersectionRatio() * currBucket.getCount()*rightOverlap.getIntersectionRatio();
		    		insertBucket.setSourceIDMap(sourceIDMap);
		    	}
		    	else {
		    		HashSet<String> sourceIds = new HashSet<String>();
			    	sourceIds.addAll(info.getCorrespondingBucket().getSourceIDs());
			    	sourceIds.addAll(currBucket.getSourceIDs());
			    	insertBucket.setSourceIDs(sourceIds);
		    	}
		    	newBuckets.add(insertBucket);
		    	//System.out.println("\t \t insert Bucket: "+insertBucket);
	    	}
	    }
	    if (0 < DEBUG) System.out.println("computeQTreeJoin(): new buckets: "+newBuckets.size());
	    // we set the maximal bucket number so that no merging should occur (add 2 "safety" buckets)
	    QTree result = new QTree(newDim,20,newBuckets.size()+2,dimSpecMin,dimSpecMax,"0","1",left.storeDetailedCounts());
//	    int inserted = 0;
	    for (UpdateBucket b : newBuckets) {
	    	//String before = result.getStateString();
	    	result.insertBucket(b,false);
	    	//inserted++;
//		    if (result.getAllBuckets().size() != inserted) {
//		    	System.out.println("\n\n\n");
//		    	System.err.println("CopyOfTestBGPJoin.computeQTreeJoin: Unexpected number of buckets in QTree: "+result.getAllBuckets().size()+" (expected: "+inserted+")");
		    	//System.out.println("newBuckets: "+newBuckets.size());
//		    	System.out.println("before: "+before);
//		    	System.out.println("inserted: "+b+"\n");
//		    	System.out.println(result.getStateString());
//		    	System.exit(1);
//		    }
	    }
	    if (result.getAllBuckets().size() != newBuckets.size()) {
	    	System.err.println("CopyOfTestBGPJoin.computeQTreeJoin: Unexpected number of buckets in QTree: "+result.getAllBuckets().size()+" (expected: "+newBuckets.size()+")");
//	    	System.out.println("newBuckets: "+newBuckets.size());
//	    	for (UpdateBucket b: newBuckets) System.out.println(b);
//	    	System.out.println(result.getStateString());
	    	System.exit(1);
	    }
	    if (1 < DEBUG) {
	    	System.out.println("qtree join result:");
	    	for (QTreeBucket b : result.getAllBuckets()) {
				if (!result.storeDetailedCounts()) System.out.println("\t"+b+", sources: "+b.getSourceIDs().size());
				else System.out.println("\t"+b+", sources: "+b.getSourceIDMap().size()+", join rank count: "+bucketCount(b));
	    	}
	    }
		
//		System.out.println("resulting tree: "+result);
	    
//		for (Map.Entry<String,Set<Space>> lnx : l.entrySet()) {
//			Set<Space> lSpace = lnx.getValue();
//			int ljcLower = lSpace.getLowerBoundaries()[lpos];
//			int ljcUpper = lSpace.getUpperBoundaries()[lpos];
//			for (Map.Entry<String,Set<Space>> rnx : r.entrySet()) {
//				Set<Space> rSpace = rnx.getValue();
//				for (Space l)
//				// I bet the following overlap computation is already somewhere available in the qtree!?
//				int rjcLower = rSpace.getLowerBoundaries()[rpos];
//				int rjcUpper = rSpace.getUpperBoundaries()[rpos];
//
//				if (overlap(ljcLower,ljcUpper,rjcLower,rjcUpper)) {
//					int[] lowerBoundaries = new int[lSpace.getLowerBoundaries().length];
//					int[] upperBoundaries = new int[lSpace.getUpperBoundaries().length];
//					// determine the intersection of the spaces
//					for (int i = 0; i < lowerBoundaries.length; ++i) {
//						lowerBoundaries[i] = lSpace.getLowerBoundaries()[i] >= rSpace.getLowerBoundaries()[i]? lSpace.getLowerBoundaries()[i] : rSpace.getLowerBoundaries()[i];
//						upperBoundaries[i] = lSpace.getUpperBoundaries()[i] <= rSpace.getUpperBoundaries()[i]? lSpace.getUpperBoundaries()[i] : rSpace.getUpperBoundaries()[i];
//					}
//					Space comb = new Space(lowerBoundaries, upperBoundaries);
//
//					String source = "l:"+lnx.getKey()+"|r:"+rnx.getKey();
//					Space currSpace = result.get(source);
//					if (null == currSpace) currSpace = comb;
//					else System.err.println("result space for "+source+" already set!?");
//					result.put(source,currSpace);
//				    if (source.contains("harth.org")) System.out.println("computeQTreeJoin: space for "+source+": "+currSpace);
//				}
//			}
//		}

		return result;
	}
	
	private TreeMap<Double,String> rankSources(QTree result, int joinDepth) {
		Map<String,Double> ranks = new HashMap<String,Double>();
		for (QTreeBucket b : result.getAllBuckets()) {
			double bucketCnt = 0.0;
			Collection<String> allSources = result.storeDetailedCounts()? b.getSourceIDMap().keySet() : b.getSourceIDs();
			for (String source : allSources) {
				double srcCnt;
				if (result.storeDetailedCounts()) {
					srcCnt = b.getSourceIDMap().get(source);
					// remove the join level from the source name - thus, we accumulate over all join levels
					if (!advancedRanking) source = source.substring(source.indexOf('|')+1);
				}
				else {
					srcCnt = (double)b.getCount()/(double)b.getSourceIDs().size();
				}
				Double cnt = ranks.get(source);
				if (null == cnt) cnt = 0.0;
				cnt += srcCnt;
				bucketCnt += srcCnt;
				ranks.put(source,cnt);
			}
//			assert 0.001 > Math.abs(bucketCnt - bucketCount(b)) : bucketCnt+" != "+bucketCount(b);
			assert 0.001 > Math.abs(bucketCnt/(double)joinDepth - b.getCount()) : bucketCnt+" != "+b.getCount();
		}
		
		TreeMap<Double,String> ranked = new TreeMap<Double, String>();
		for (Map.Entry<String,Double> sources : ranks.entrySet()) {
			String sourceName = sources.getKey();
			double cnt = sources.getValue();
//			while (ranked.containsKey(cnt)) cnt += 0.000001;
			while (ranked.containsKey(cnt)) cnt += 0.001;
			ranked.put(cnt,sourceName);
		}
		//		System.out.println(ranked);
		return ranked;
	}
	
	private Vector<String> getRankOrderedSources(TreeMap<Double,String> ranked, int joinDepth) {
		Vector<String> orderedSources = new Vector<String>();
		if (!advancedRanking) {
			for (Iterator<String> it = ranked.descendingMap().values().iterator(); it.hasNext(); ) {
				orderedSources.add(it.next());
			}
		}
		else {
			Vector<String>[] advRanks = new Vector[joinDepth];
			for (Iterator<String> it = ranked.descendingMap().values().iterator(); it.hasNext(); ) {
				String source = it.next();
				int level = new Integer(source.substring(0,source.indexOf('|'))) - 1;
				source = source.substring(source.indexOf('|')+1);
				if (null == advRanks[level]) advRanks[level] = new Vector<String>();
				advRanks[level].add(source);
			}
			int currLvl = -1;
			int empty = 0;
			while (advRanks.length > empty) {
				currLvl = (++currLvl) % joinDepth;
				if (advRanks[currLvl].isEmpty()) continue;
				String nextSource = advRanks[currLvl].remove(0);
				if (advRanks[currLvl].isEmpty()) ++empty;
				if (!orderedSources.contains(nextSource)) orderedSources.add(nextSource);
			}
		}
		
		return orderedSources;
	}
	
	private Double computeTopKPercentage(TreeMap<Double,String> ranked, int k, Map<Node[],String> resTriples) throws Exception {
		// this would speed up the calculation - currently, we can also check if the whole set really contains all actually relevant sources
		//			if (topK[k]<=ranked.size()) topKPerc[k] = 1.0;
		// determine the top-kth key
		double fromKey = ranked.firstKey();
		// if not, the top-kth key is the lowest (i.e., the first) in the ranked set
		if (k<=ranked.size()) {
			NavigableSet<Double> keys = ranked.descendingKeySet();
			//				for (int z=0; z<topK[k]-1; ++z) keys.pollFirst();
			//				fromKey = keys.pollFirst();
			// iterate over all keys descending from the end of the set
			Iterator<Double> it = keys.iterator();
			// skip the k-1 top keys
			for (int z=0; z<k-1; ++z) it.next();
			// that's the top-kth key now
			fromKey = it.next();
		}
		//			System.out.println("tail-"+topK[k]+": "+ranked.tailMap(fromKey).size()+": "+ranked.tailMap(fromKey));
		int resTriple = 0;
		// iterate over all result triples 
		for (Map.Entry<Node[],String> nx : resTriples.entrySet()) {
			// extract the single sources contributing to the result triple
			StringTokenizer tok = new StringTokenizer(nx.getValue(),"|");
			boolean allSourcesIncl = true;
			// check if all contributing sources are in the top-k sources set
			while (tok.hasMoreTokens() && allSourcesIncl) {
				if (!ranked.tailMap(fromKey).values().contains(URLDecoder.decode(tok.nextToken(), "utf-8"))) allSourcesIncl = false;
			}
			// are all contributing sources included in the top-k sources set?
			if (allSourcesIncl) ++resTriple;
		}
		// determine the actual percentage we could get when only using the top-k sources
		return (double)resTriple/(double)resTriples.size();
	}
		
	private Double computeTopKPercentage(Vector<String> ranked, int k, Map<Node[],String> resTriples) throws Exception {
		// this would speed up the calculation - currently, we can also check if the whole set really contains all actually relevant sources
		//			if (topK[k]<=ranked.size()) topKPerc[k] = 1.0;
		int resTriple = 0;
		// iterate over all result triples 
		for (Map.Entry<Node[],String> nx : resTriples.entrySet()) {
			// extract the single sources contributing to the result triple
			StringTokenizer tok = new StringTokenizer(nx.getValue(),"|");
			boolean allSourcesIncl = true;
			// check if all contributing sources are in the top-k sources set
			while (tok.hasMoreTokens() && allSourcesIncl) {
				int pos = ranked.indexOf((URLDecoder.decode(tok.nextToken(), "utf-8")));
				if (-1 == pos || k <= pos) allSourcesIncl = false;
			}
			// are all contributing sources included in the top-k sources set?
			if (allSourcesIncl) ++resTriple;
		}
		// determine the actual percentage we could get when only using the top-k sources
		return (double)resTriple/(double)resTriples.size();
	}

	private Map<String,Integer> getSourceRanks(Map<Node[],String> resultTriples) throws Exception {
		Map<String,Double> ranks = new HashMap<String,Double>();
		int tokCnt = 0;
		for (Map.Entry<Node[],String> nx : resultTriples.entrySet()) {
			StringTokenizer tok = new StringTokenizer(nx.getValue(),"|");
			while (tok.hasMoreTokens()) {
				// we only have to count once
				if (0 == tokCnt) ++tokCnt;
				String source = URLDecoder.decode(tok.nextToken(), "utf-8");
				Double cnt = ranks.get(source);
				if (null == cnt) cnt = 0.0;
				cnt += 1.0;
				ranks.put(source,cnt);
			}
		}
		TreeMap<Double,String> ranked = new TreeMap<Double, String>();
		for (Map.Entry<String,Double> sources : ranks.entrySet()) {
			String sourceName = sources.getKey();
			double cnt = sources.getValue();
			// this is one approach: 30 result triples, 3 sources -> assign 10 to each source...
//			cnt /= (double)tokCnt;
			// ...actually, the cummulated one works better: 30 result triples, 3 sources -> assign 30 to each source (i.e., sum(assignment)=#restriples*#joinlevel)
			while (ranked.containsKey(cnt)) cnt += 0.000001;
			ranked.put(cnt,sourceName);
		}
		Map<String,Integer> sourceRanks = new HashMap<String,Integer>();
		int cnt = 0;
		for (String s : ranked.values()) {
			sourceRanks.put(s,ranked.size()-cnt);
			++cnt;
		}
//		System.out.println(ranked);
		return sourceRanks;
	}
	
	private double getRankError(TreeMap<Double,String> ranked, Map<Node[],String> resTriples, Vector<Integer> positions) throws Exception {
		Map<String,Integer> actRanks = getSourceRanks(resTriples);
		double avgErr = 0.0;
		int cnt = 0;
		int notFound = 0;
		System.out.print("rank errors: ");
		for (String s : ranked.values()) {
			if (advancedRanking) s = s.substring(s.indexOf('|')+1);
			if (actRanks.containsKey(s)) {
				int qtreeRank = ranked.size()-cnt;
				positions.add(qtreeRank);
//				System.out.println("QTree rank="+qtreeRank+"; actual rank="+actRanks.get(s)+"; error="+Math.abs(qtreeRank-actRanks.get(s)));
				System.out.print("|"+qtreeRank+"-"+actRanks.get(s)+"|; ");
				avgErr += Math.abs(qtreeRank-actRanks.get(s));
			}
			else ++notFound;
			++cnt;
		}
		System.out.println();
		// average over only the actually contained sources
		avgErr /= (double)(ranked.size()-notFound);
		return avgErr;
	}
	
	private double getRankError(Vector<String> ranked, Map<Node[],String> resTriples, Vector<Integer> positions) throws Exception {
		Map<String,Integer> actRanks = getSourceRanks(resTriples);
		double avgErr = 0.0;
		int cnt = 0;
		int notFound = 0;
		System.out.print("rank errors: ");
		for (String s : ranked) {
			if (actRanks.containsKey(s)) {
				int qtreeRank = cnt+1;
				positions.add(qtreeRank);
//				System.out.println("QTree rank="+qtreeRank+"; actual rank="+actRanks.get(s)+"; error="+Math.abs(qtreeRank-actRanks.get(s)));
				System.out.print("|"+qtreeRank+"-"+actRanks.get(s)+"|; ");
				avgErr += Math.abs(qtreeRank-actRanks.get(s));
			}
			else ++notFound;
			++cnt;
		}
		System.out.println();
		// average over only the actually contained sources
		avgErr /= (double)(ranked.size()-notFound);
		return avgErr;
	}

	private int getMaxK(TreeMap<Double,String> ranked, Set<String> allResSources, Vector<Integer> positions) {
		int cnt = 0;
		while (!ranked.isEmpty()) {
			// increment the k
			++cnt;
			Double lastKey = ranked.lastKey();
			// that's the kth source
			String s = ranked.get(lastKey);
			if (advancedRanking) s = s.substring(s.indexOf('|')+1);
			//			System.out.println("checking: "+s+" vs. "+allResSources);
			// if the kth source is in the set of actual result sources, remove it from there
			if (allResSources.contains(s)) {
				//				System.out.println("found "+s+" at position "+cnt);
				positions.add(cnt);
				allResSources.remove(s);
			}
			// as soon as all actual result sources are found, we found the k
			if (allResSources.isEmpty()) break;
			// remove the checked source from the end of the ranked set
			ranked.remove(lastKey);
		}
		return cnt;
	}
	
	private double bucketCount(Bucket b) {
//		if (0 < b.getCount()) return b.getCount();
		double cnt = 0.0;
		for (Double d : b.getSourceIDMap().values()) {
			cnt += d;
		}
		return cnt;
	}

	public Map<Node[],String> computeJoin(Map<Node[],String> l, int lpos, Map<Node[],String> r, int rpos) {
		Map<Node[],String> result = new HashMap<Node[],String>();
		
		for (Map.Entry<Node[],String> lnx : l.entrySet()) {
			Node ljc = lnx.getKey()[lpos];
			for (Map.Entry<Node[],String> rnx : r.entrySet()) {
				Node rjc = rnx.getKey()[rpos];
				
				if (ljc.equals(rjc)) {
					Node[] comb = new Node[lnx.getKey().length+rnx.getKey().length];
					System.arraycopy(lnx.getKey(), 0, comb, 0, lnx.getKey().length);
					System.arraycopy(rnx.getKey(), 0, comb, lnx.getKey().length, rnx.getKey().length);

//					result.put(comb,"l:"+lnx.getValue()+"|r:"+rnx.getValue());
					result.put(comb,lnx.getValue()+"|"+rnx.getValue());
				}
			}
		}
		
		return result;
	}

	public Map<Node[],String> evaluateBgp(String[] sources, Node[] bgp) throws FileNotFoundException, ParseException, IOException {
		BGPMatcher m = new BGPMatcher(bgp);

		System.out.println("Query for " + Nodes.toN3(bgp));

		Map<Node[],String> results = new HashMap<Node[],String>();

		for (String s : sources) {
			String baseurl = URLDecoder.decode(s, "utf-8");

			File f = new File(DATA_DIR + s);
			if (f.isFile()) {
				RDFXMLParser r = new RDFXMLParser(new FileInputStream(DATA_DIR + s), baseurl);
				while (r.hasNext()) {
					Node[] nx = r.next();
					if (m.match(nx)) {
						results.put(nx,s);
//						System.out.println(s+": "+Nodes.toN3(nx));
					}
				}
			}
		}
		
		return results;
	}

	private void buildOrLoad(File outputDir, OnDiskOne4AllQTreeIndex index, File inputIDX) throws IOException {
    	outputDir.mkdirs();	   
    	File qtreeFile = new File(outputDir,index.getFileName());
    	if(qtreeFile.exists()){
    	    System.err.println("[DEBUG] Found serialised version of a qtree in the output dir "+outputDir);
    	    index.loadQTreeIndex(outputDir);
    	    
    	}
    	else{
    	    System.err.println("[DEBUG] Could not found serialised version of a qtree in the output dir "+outputDir);
    	    System.err.println("[DEBUG] Building new QTree from statements in "+ inputIDX);
    	    
    	    InsertCallback c = new InsertCallback(index);
		    NodeBlockInputStream nbis = new NodeBlockInputStream(inputIDX.getAbsolutePath());
		    QuadStringScanIterator iter = new QuadStringScanIterator(nbis);
		    int count=0;
		    while(iter.hasNext()){
		    	//System.out.print(".");
		    	c.processStatement(iter.next());
		    	count++;
		    	if(count%10000==0)
		    		System.err.println("DEBUG inserted "+count+" stmts");
		    }
		    System.out.println("inserted "+c.insertedStatments()+" stmts");
		    System.err.println("[DEBUG] Serialising QTree from statements to "+ outputDir);
		    	   
		    index.serialiseQTree(outputDir);
    	}
	}
}
