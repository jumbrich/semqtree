package ie.deri.urq.realidy.query.qtree;

import static de.ilmenau.datasum.index.AbstractIndex.VARIABLE;
import ie.deri.urq.realidy.bench.utils.MemoryMonitor;
import ie.deri.urq.realidy.query.arq.QueryParser;
import ie.deri.urq.realidy.query.qtree.operator.InvListQTreeJoin;
import ie.deri.urq.realidy.query.qtree.operator.NestedLoopQTreeJoin;
import ie.deri.urq.realidy.query.qtree.operator.QTreeJoinOperator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;
import java.util.Map.Entry;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.stats.Count;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.ilmenau.datasum.index.AbstractIndex;
import de.ilmenau.datasum.index.Bucket;
import de.ilmenau.datasum.index.IntersectionInformation;
import de.ilmenau.datasum.index.JoinBucket;
import de.ilmenau.datasum.index.JoinSpace;
import de.ilmenau.datasum.index.QueryResultEstimation;
import de.ilmenau.datasum.index.QuerySpace;
import de.ilmenau.datasum.index.SingleMultiDimHistogramIndex;
import de.ilmenau.datasum.index.SingleQTreeIndex;
import de.ilmenau.datasum.index.qtree.QTree;
import de.ilmenau.datasum.index.qtree.QTreeBucket;

public class QueryProcessor {
	private final static Logger log = LoggerFactory.getLogger(QueryProcessor.class);
		
	private AbstractIndex _idx;
	private boolean storeDetailCount;
	private QueryParser _qp = new QueryParser();
	private boolean _debug;
	private QTreeJoinOperator op = new NestedLoopQTreeJoin();
//	private final QTreeJoinOperator op = new InvListQTreeJoin();

	private File jbdOut;

	private boolean _jbDenabled;
	public QueryProcessor(AbstractIndex idx) {
		_idx = idx;
		storeDetailCount = _idx.getStoreDetailedCount();
	}

	public QueryResultEstimation executeQuery(Node[][] bgps,
			int[][] bgpsHashCoordinates, boolean determineResultingBuckets, boolean reordering) {
		long start = System.currentTimeMillis();
		boolean expectResults = true;
		log.info("Running query now ");
		QueryResultEstimation result = new QueryResultEstimation(bgpsHashCoordinates.length, _debug);
		result.setBGP(bgps);
		result.setOldIndices(bgpsHashCoordinates);
		List<List<Bucket>> bgpBuckets = executeBGPLookups(bgpsHashCoordinates,result);
		
		int [] noOfBuckets = new int[bgpsHashCoordinates.length];
		for(int i=0; i < bgpBuckets.size();i++){
			noOfBuckets[i] = bgpBuckets.get(i).size();
			if(_debug) log.info(" TRP+"+i+"= "+noOfBuckets[i]+" buckets");
			if(noOfBuckets[i]==0){
				expectResults = false;
				System.out.println("FALSE NO RESULTS FOR BGP");
			}
			
		}
		if(!expectResults) return result;
		
		int [][] joinIndicesOld = _qp.findJoins(bgps);
		Integer [] oldJoinOrder = new Integer[bgps.length];
		result.setOldJoinOrder(oldJoinOrder);
		for(int i =0; i < bgps.length;i++){
			oldJoinOrder[i]=i;
		}
		Integer[] newJoinOrder = oldJoinOrder;
		int [][] joinIndices = joinIndicesOld;
		
		long start1 = System.currentTimeMillis();
		if(reordering){
			//do the reordering of the joins to reduce the number of buckets we have to compare for overlaps
			
			if(_idx instanceof SingleQTreeIndex){
				newJoinOrder = QTreeQueryOptimiser.optimise(bgps, noOfBuckets,((SingleQTreeIndex)_idx)._sourcesQTree.getMaxBuckets());
			}
			else if(_idx instanceof SingleMultiDimHistogramIndex){
				newJoinOrder = QTreeQueryOptimiser.optimise(bgps, noOfBuckets,((SingleMultiDimHistogramIndex)_idx)._multiDimHist.getMaxBuckets());
			}
			result.setOrderingTime(System.currentTimeMillis()-start1);
			if(newJoinOrder==null || newJoinOrder.length==0) return result;
		
			joinIndices= _qp.findJoins(reorder(bgps,newJoinOrder));
			result.setJoinOrder(newJoinOrder);
			result.setJoinIndices(joinIndices);
			result.setOrderingTime(System.currentTimeMillis()-start1);
		}else{
			result.setOrderingTime(-1);
			result.setJoinOrder(new Integer[0]);
			result.setJoinIndices(new int[0][0]);
		}
		if(_debug){
			log.info("  INPUT QUERY [INDEX] [BUCKET] [N3] [HASH]");
			for(int i =0; i < bgpBuckets.size();i++){
				log.info(" [{}] [{}] [{}] [{}]",new Object[]{i,noOfBuckets[i],Nodes.toN3(bgps[i]),Arrays.toString(bgpsHashCoordinates[i])});
			}
			log.info(" ->Old Join Indices");
			for(int [] join: joinIndicesOld){
				log.info(" -->{}",Arrays.toString(join));
			}
			log.info("New joinOrder: {}",Arrays.toString(newJoinOrder));
			for(int [] join: joinIndices){
				log.info(" -->{}",Arrays.toString(join));
			}
		}
		
		for(int i =0; i < newJoinOrder.length;i++){
			if (determineResultingBuckets) 
				result.setBgpResultingBuckets(noOfBuckets[newJoinOrder[i]], i);
				result.setBgpEstSources(countSources(bgpBuckets.get(newJoinOrder[i]),storeDetailCount),i);
		}
		
		
		
		//lets start the join
		JoinSpace currResult = new JoinSpace(3);
		start1 = System.currentTimeMillis();

//		if(_debug) log.info("Join processing");
		currResult.add((ArrayList<Bucket>) bgpBuckets.get(newJoinOrder[0]));
//		if(_debug) log.info("Resulting buckets for first lookup");
//		for(Bucket b : currResult.getBuckets()){
//			System.out.println(b);
//		}
		// if the left result is empty, the whole join is empty
		if (null != currResult) {
			for (int j = 1; j < bgpsHashCoordinates.length; j++) {
				start1 = System.currentTimeMillis();
				
				if(_jbDenabled)op.resetJoiNBucketDist();
				currResult = op.execute(_idx,currResult, joinIndices[j-1][0], (ArrayList<Bucket>) bgpBuckets.get(newJoinOrder[j] ), joinIndices[j-1][1], j, storeDetailCount);
				if(_jbDenabled) printJoinBucketDist(op.getJoinBucketDist(),j);
//				log.info("Join {} has src {}",new Object[]{j,countSources(currResult.getBuckets(),storeDetailCount)});
				if (determineResultingBuckets){ result.setJoinResultingBuckets(currResult.bucketCount(), j-1);}
				result.setJoinEvalTime(System.currentTimeMillis()-start1,j-1);
			}
		}
		result.setTotalQueryTime(System.currentTimeMillis()-start);
		start1 = System.currentTimeMillis();

		SourceRankMap ranked = rankSources(currResult, joinIndices.length+1,false);

		result.setRelevantSourcesRanked(getRankOrderedSources(ranked, joinIndices.length+1,false));
		result.setRankTime(System.currentTimeMillis()-start1);

		ranked = null;
		currResult = null;

		result.setOperator(op.getClass().getSimpleName());
		
		return result;
	}

	private void printJoinBucketDist(Count<Integer> joinBucketDist, int join) {
		File out = new File(jbdOut, "joinBucketDist-"+join+".dat");
		
		PrintStream w;
		try {
			w = new PrintStream(out);
			joinBucketDist.printOrderedStats(w);
			w.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		
	}

	private List<List<Bucket>> executeBGPLookups(int[][] bgpsHashCoordinates, QueryResultEstimation result) {
		 List<List<Bucket>> res = new ArrayList<List<Bucket>>(bgpsHashCoordinates.length);

		int c =0;
		long start1 = System.currentTimeMillis();
		Vector<IntersectionInformation> v = null;
		for(int [] tripPattern: bgpsHashCoordinates){
			v = _idx.getAllBucketsInQuerySpace(getQuerySpace(tripPattern,false)); 
			result.setBgpEvalTime(System.currentTimeMillis()-start1,c);
			
			res.add(prepareForQTreeJoin(v,null));
			
			start1= System.currentTimeMillis();
			c++;
		}
		return res;
	}

	private Node[][] reorder(Node[][] bgps, Integer[] newJoinOrder) {
		Node [][] n = new Node[bgps.length][];
		for(int i=0; i < newJoinOrder.length;i++){
			n[i] = bgps[newJoinOrder[i]];
		}
		return n;
	}

	/**
	 * 
	 * @param q
	 * @param hasNoRestrictions
	 * @return
	 */
	private  QuerySpace getQuerySpace(int[] hashCoordinates, boolean hasNoRestrictions) {
		int[] lowerBoundaries = Arrays.copyOf(hashCoordinates,hashCoordinates.length);
		int[] upperBoundaries = Arrays.copyOf(hashCoordinates,hashCoordinates.length);
		for (int i=0;i<hashCoordinates.length;i++) {
			if (hashCoordinates[i]== VARIABLE) {
				// variables
				lowerBoundaries[i] = _idx.getDimSpecMin()[i];
				upperBoundaries[i] = _idx.getDimSpecMax()[i];
			}
		}
		return new QuerySpace(lowerBoundaries, upperBoundaries, hasNoRestrictions);
	}

	private int countSources(List<Bucket> v, boolean storeDetailedCounts) {
		Set<String> s = new HashSet<String>();
		for(Bucket b: v){
			if (!storeDetailedCounts) s.addAll(b.getSourceIDs());
			else s.addAll(b.getSourceIDMap().keySet());
		}
		int size = s.size();
		s=null;
		return size;
		
	}
//	private int countSources(Vector<IntersectionInformation> v, boolean storeDetailedCounts) {
//	
//		Set<String> s = new HashSet<String>();
//		for (IntersectionInformation i : v) {
//			if (!storeDetailedCounts) s.addAll(i.getCorrespondingBucket().getSourceIDs());
//			else s.addAll(i.getCorrespondingBucket().getSourceIDMap().keySet());
//		}
//		return s.size();
//	}



	private JoinSpace computeQTreeJoin(JoinSpace left, int lpos,ArrayList<Bucket> right, int rpos, int joinLevel, boolean useParentJoinSpace) {
		int currDim = left.getNmbOfDimensions();
		final int newDim = currDim+3;
		JoinSpace result = new JoinSpace(newDim);

		int newBucketCnt = 0;

		log.info(">>COMPUTE QTREE JOIN-{} lpos:{} lb:{} rpos:{} rb:{}",new Object[] {joinLevel,lpos,left.getBuckets().size(),rpos,right.size()});
		
		// determine the actual overlap with all intersection spaces from right
		for (Iterator<Bucket> rightIt = right.iterator(); rightIt.hasNext(); ) {
			Bucket currBucket = rightIt.next(); rightIt.remove();
// System.err.println("\n  BUCKET: "+currBucket);
// System.err.println("  currDim: "+currDim);
			int[] lowerBoundaries = new int[currDim];
			int[] upperBoundaries = new int[currDim];
			for (int i=0; i<currDim; ++i) {
				// only restrict the join dimension
				if (i == lpos) {
					// use the boundaries of the right tree to limit the buckets of the left tree
					// should be the same vice versa
					lowerBoundaries[i] = currBucket.getLowerBoundaries()[rpos];
					upperBoundaries[i] = currBucket.getUpperBoundaries()[rpos];
				}
				else {
					// all other dimensions are united without restrictions
					lowerBoundaries[i] = _idx.getDimSpecMin()[3%1];
					upperBoundaries[i] = _idx.getDimSpecMax()[3%1];
				}
			}
			//			if(_debug)System.err.println("  "+joinLevel+" lower: "+Arrays.toString(lowerBoundaries));
			//			if(_debug)System.err.println("  "+joinLevel+" upper: "+Arrays.toString(upperBoundaries));
			QuerySpace querySpace = new QuerySpace(lowerBoundaries, upperBoundaries, false);
//			System.err.println("  QUERYSPACE low:"+lowerBoundaries.length+" up:"+upperBoundaries.length+":"+querySpace);
			// TODO: does this handle redundant buckets accordingly?
			ArrayList<IntersectionInformation> overlap = left.getAllBucketsInQuerySpace(querySpace);
//			System.err.println("  Overlap:"+overlap);
			
			
			for (Iterator<IntersectionInformation> infoIt = overlap.iterator(); infoIt.hasNext(); ) {
				IntersectionInformation info = infoIt.next(); infoIt.remove();
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
						rightLowerBoundaries[i] = _idx.getDimSpecMin()[3%1];
						rightUpperBoundaries[i] = _idx.getDimSpecMax()[3%1];
					}
				}
				querySpace = new QuerySpace(rightLowerBoundaries, rightUpperBoundaries, false);
				//System.out.println("\t \t querySpace: "+querySpace.toString());
				IntersectionInformation rightOverlap = JoinSpace.computeIntersectionInformation(currBucket,querySpace);
				JoinBucket insertBucket; 
				double range = info.getCorrespondingBucket().getUpperBoundaries()[lpos] - info.getCorrespondingBucket().getLowerBoundaries()[lpos];
				if ((currBucket.getUpperBoundaries()[rpos] - currBucket.getLowerBoundaries()[rpos]) > range) range = currBucket.getUpperBoundaries()[rpos] - currBucket.getLowerBoundaries()[rpos];
				if (0.0 == range) range = 1.0;
				
				
				
				if (storeDetailCount) {
					// only for the assert check
					double bucketCnt = 0.0;
					HashMap<String,Double> sourceIDMap = new HashMap<String, Double>();
					// set sources and counts from the current buckets of the left input QTree
					if (info.getCorrespondingBucket() instanceof JoinBucket && null == info.getCorrespondingBucket().getSourceIDMap()) {
						//		    			System.err.println("1: Yeappa!!!");
						JoinBucket joinBucket = (JoinBucket)info.getCorrespondingBucket();
						for (int i = 0; i < left.getNrOfSources(); ++i) {
							double cnt = joinBucket.getSourceCnt(i);
							if (-1 == cnt) break;
							if (0.0 < cnt) {
								// it's a cross product between left and right sides
								double newCnt = (cnt*info.getIntersectionRatio() * currBucket.getCount()*rightOverlap.getIntersectionRatio()) / range;
								bucketCnt += newCnt;
								sourceIDMap.put(left.getSource(i),newCnt);
							}
						}
						if (!rightIt.hasNext()) joinBucket.clear();
					}
					else {
						for (Iterator<Map.Entry<String,Double>> entryIt = info.getCorrespondingBucket().getSourceIDMap().entrySet().iterator(); entryIt.hasNext(); ) {
							Map.Entry<String,Double> e = entryIt.next();
							if (!rightIt.hasNext()) entryIt.remove();
							// it's a cross product between left and right sides
							//		    			double newCnt = e.getValue()*info.getIntersectionRatio() * bucketCount(currBucket)*rightOverlap.getIntersectionRatio();
							double newCnt = (e.getValue()*info.getIntersectionRatio() * currBucket.getCount()*rightOverlap.getIntersectionRatio()) / range;
							//		    			System.out.println("1:"+e.getValue()+"*"+info.getIntersectionRatio()+" * "+currBucket.getCount()+"*"+rightOverlap.getIntersectionRatio());

							// divide by 2 because it's only one side of the join
							// this is one approach: 30 result triples, 3 sources -> assign 10 to each source...
							//		    			newCnt /= 2.0;
							// ...actually, the accumulated one works better: 30 result triples, 3 sources -> assign 30 to each source (i.e., sum(assignment)=#restriples*#joinlevel)

							//		    			System.out.println("newCnt: "+newCnt);
							bucketCnt += newCnt;
							sourceIDMap.put(e.getKey(),newCnt);
						}
						if (!rightIt.hasNext()) info.getCorrespondingBucket().sourceIDCountMap = null;
					}
					// iterate over the buckets from the right input QTree and set sources and counts
					if (currBucket instanceof JoinBucket && null == currBucket.getSourceIDMap()) {
						//		    			System.err.println("2: Yeappa!!!");
						JoinBucket joinBucket = (JoinBucket)currBucket;
						for (int i = 0; i < left.getNrOfSources(); ++i) {
							double cnt = joinBucket.getSourceCnt(i);
							if (-1 == cnt) break;
							if (0.0 < cnt) {
								double newCnt = (info.getCorrespondingBucket().getCount()*info.getIntersectionRatio() * cnt*rightOverlap.getIntersectionRatio()) / range;
								bucketCnt += newCnt;
								String source = left.getSource(i);
								if (sourceIDMap.containsKey(source)) newCnt += sourceIDMap.get(source);
								sourceIDMap.put(source,newCnt);
							}
						}
					}
					else {
						for (Iterator<Map.Entry<String,Double>> entryIt = currBucket.getSourceIDMap().entrySet().iterator(); entryIt.hasNext(); ) {
							Map.Entry<String,Double> e = entryIt.next();
							//		    			if (!infoIt.hasNext()) entryIt.remove();
							// we have to divide by joinLevel, cause we accumulate (i.e., 30 triples, 3 sources -> bucketCount()=90)!
							//		    			double newCnt = bucketCount(info.getCorrespondingBucket())/(joinLevel)*info.getIntersectionRatio() * e.getValue()*rightOverlap.getIntersectionRatio();
							double newCnt = (info.getCorrespondingBucket().getCount()*info.getIntersectionRatio() * e.getValue()*rightOverlap.getIntersectionRatio()) / range;
							//		    			System.out.println("2:"+info.getCorrespondingBucket().getCount()+"*"+info.getIntersectionRatio()+" * "+e.getValue()+"*"+rightOverlap.getIntersectionRatio());

							// divide by 2 because it's only one side of the join
							// this is one approach: 30 result triples, 3 sources -> assign 10 to each source...
							//		    			newCnt /= 2.0;
							// ...actually, the accumulated one works better: 30 result triples, 3 sources -> assign 30 to each source (i.e., sum(assignment)=#restriples*#joinlevel)

							//		    			System.out.println("newCnt: "+newCnt);
							bucketCnt += newCnt;
							// if the source already exists in the left input, it has a different join level - thus, we maintain counts for different levels separately
							String source = e.getKey();
							if (sourceIDMap.containsKey(source)) newCnt += sourceIDMap.get(source);
							sourceIDMap.put(source,newCnt);
						}
						//		    		if (!infoIt.hasNext()) currBucket.sourceIDCountMap = null;
					}
					//					assert 0.001 > Math.abs(bucketCnt - bucketCount(info.getCorrespondingBucket())/joinLevel*info.getIntersectionRatio() * bucketCount(currBucket)*rightOverlap.getIntersectionRatio() * (joinLevel+1)) : 
					//						bucketCnt+" != "+bucketCount(info.getCorrespondingBucket())/joinLevel*info.getIntersectionRatio() * bucketCount(currBucket)*rightOverlap.getIntersectionRatio() * (joinLevel+1);
					assert 0.001 > Math.abs(bucketCnt/(double)(joinLevel+1) - (info.getCorrespondingBucket().getCount()*info.getIntersectionRatio() * currBucket.getCount()*rightOverlap.getIntersectionRatio()) / range) : 
						bucketCnt/(double)(joinLevel+1)+" != "+(info.getCorrespondingBucket().getCount()*info.getIntersectionRatio() * currBucket.getCount()*rightOverlap.getIntersectionRatio()) / range;

					JoinSpace parentSpace = useParentJoinSpace? result : null;
					insertBucket = new JoinBucket(lowerBoundaries,upperBoundaries,
							// it's a cross product between left and right sides
							(info.getCorrespondingBucket().getCount()*info.getIntersectionRatio() * currBucket.getCount()*rightOverlap.getIntersectionRatio()) / range,
							sourceIDMap,parentSpace);
					sourceIDMap.clear();
					sourceIDMap = null;
				}
				else {
					HashSet<String> sourceIds = new HashSet<String>();
					sourceIds.addAll(info.getCorrespondingBucket().getSourceIDs());
					sourceIds.addAll(currBucket.getSourceIDs());
					insertBucket = new JoinBucket(lowerBoundaries,upperBoundaries,
							// it's a cross product between left and right sides
							(info.getCorrespondingBucket().getCount()*info.getIntersectionRatio() * currBucket.getCount()*rightOverlap.getIntersectionRatio()) / range,
							sourceIds);
				}
				info = null;
				result.add(insertBucket);
				++newBucketCnt;
			}
//			System.err.println("  Intersection info iteration end");
			currBucket = null;
			overlap = null;
		}
//		System.err.println("Iteration over right end");
		left.clear();
		right.clear();
		right = null;
		left = null;

		//		//    	if (result.getAllBuckets().size() != newBucketCnt) {
		//		//    		logger.warn("OnDiskOne4AllQTreeIndex.computeQTreeJoin: Unexpected number of buckets in QTree: "+result.getAllBuckets().size()+" (expected: "+newBucketCnt+")");
		//		//    	}
		//		if (_idx.get < newBucketCnt) {
		//			log.info("Exceeded original number of buckets in QTree: "+newBucketCnt+" (original: "+buckets+")");
		//		}
		return result;
	}

	private ArrayList<Bucket> prepareForQTreeJoin(Vector<IntersectionInformation> spaces, JoinSpace joinSpace)  {
		if (0 == spaces.size()) return new ArrayList<Bucket>(0);
		ArrayList<Bucket> buckets = new ArrayList<Bucket>();

		// insert the spaces from left into a temporary QTree in order to determine the overlap in the join dimension
		Bucket currBucket;
		for (IntersectionInformation interSec : spaces) {
			Bucket orgBucket = interSec.getCorrespondingBucket();
//			System.out.println(Arrays.toString(orgBucket.getLowerBoundaries()));
			if (storeDetailCount) {
				double bucketCnt = 0.0;
				HashMap<String,Double> sourceIDMap = new HashMap<String, Double>();
				for (Map.Entry<String,Double> e : orgBucket.getSourceIDMap().entrySet()) {
					double newCnt = e.getValue()*interSec.getIntersectionRatio();
					bucketCnt += newCnt;
					sourceIDMap.put(e.getKey(),newCnt);
				}
				assert 0.001 > Math.abs(bucketCnt - sumSourceCounts(orgBucket)*interSec.getIntersectionRatio()) : bucketCnt+" != "+sumSourceCounts(orgBucket)*interSec.getIntersectionRatio();
				currBucket = new JoinBucket(orgBucket.getLowerBoundaries(), orgBucket.getUpperBoundaries(),orgBucket.getCount()*interSec.getIntersectionRatio(),sourceIDMap,joinSpace);
			}
			else {
				currBucket = new JoinBucket(orgBucket.getLowerBoundaries(), orgBucket.getUpperBoundaries(),orgBucket.getCount()*interSec.getIntersectionRatio(),orgBucket.getSourceIDs());
			}
			buckets.add(currBucket);
		}
		
		return buckets;
	}



	/**
	 * sums the values over all sources in the sourceIdMap
	 * 
	 * @param b
	 * @return
	 */
	private double sumSourceCounts(Bucket b) {
		//		if (0 < b.getCount()) return b.getCount();
		double cnt = 0.0;
		for (Double d : b.getSourceIDMap().values()) {
			cnt += d;
		}
		return cnt;
	}

	/**
	 * ranks the sources from the QTree, result of a query estimation
	 * 
	 * @param result
	 * @param joinDepth
	 * @return A TreeMap m with a value representing the rank
	 *  if m[i,x] & m[j,y] with i<j, then is y higher ranked than x
	 *  map is sorted ascending, thus the lowest ranked source comes first!
	 */
	private SourceRankMap rankSources(QTree result, int joinDepth, boolean advancedRanking) {
		SourceRankMap ranks = new SourceRankMap();
		// first, collect the rank values for each source
		for (QTreeBucket b : result.getAllBuckets()) {
			// only for the assert check
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
					// assume uniform distribution if no detailed count is stored
					srcCnt = (double)b.getCount()/(double)b.getSourceIDs().size();
				}
				Double cnt = ranks.get(source);
				if (null == cnt) cnt = 0.0;
				cnt += srcCnt;
				bucketCnt += srcCnt;
				ranks.put(source,cnt);
			}
			//			assert 0.001 > Math.abs(bucketCnt - bucketCount(b)) : bucketCnt+" != "+bucketCount(b);
			assert 0.001 > Math.abs(bucketCnt/(double)joinDepth - b.getCount()) : bucketCnt/(double)joinDepth+" != "+b.getCount();
		}

		//		// now, switch key and values - thus, we use the sorting of the TreeMap to do the actual ranking
		//		TreeMap<Double,String> ranked = new TreeMap<Double, String>();
		//		for (Map.Entry<String,Double> sources : ranks.entrySet()) {
		//			String sourceName = sources.getKey();
		//			double cnt = sources.getValue();
		//			// there may be equally ranked sources - just add a minor value for them
		//			// TODO: I know, it's not perfect...! had a much smaller value in the beginning, resulted in an endless loop for very huge counts (+0.000001 has no effect!?!)
		//			while (ranked.containsKey(cnt)) cnt += 0.001;
		//			ranked.put(cnt,sourceName);
		//		}
		//		//		System.out.println(ranked);
		return ranks;
	}

	private SourceRankMap rankSources(JoinSpace result, int joinDepth, boolean advancedRanking) {
		long start = System.currentTimeMillis();
		log.info("Ranking sources from {} buckets and {} sources",new Object[]{result.getBuckets().size(),countSources(result.getBuckets(),storeDetailCount)});
		
		SourceRankMap ranks = new SourceRankMap();
		// first, collect the rank values for each source
		for (Bucket b : result.getBuckets()) {
			// only for the assert check
			double bucketCnt = 0.0;
			double srcCnt;
			if (storeDetailCount) {
				if (b instanceof JoinBucket && null == b.getSourceIDMap()) {
					JoinBucket joinBucket = (JoinBucket)b;
					for (int i=0; i<result.getNrOfSources(); ++i) {
						srcCnt = joinBucket.getSourceCnt(i);
						if (-1.0 == srcCnt) break;
						if (0.0 < srcCnt) {
							String source = result.getSource(i);
							if (!advancedRanking && source.contains("|")) source = source.substring(source.indexOf('|')+1);
							Double cnt = ranks.get(source);
							if (null == cnt) cnt = 0.0;
							cnt += srcCnt;
							bucketCnt += srcCnt;
							ranks.put(source,cnt);
						}
					}
				}
				else {
					for(Entry<String, Double> ent: b.getSourceIDMap().entrySet()){
						srcCnt = ent.getValue();
						String source = ent.getKey();
						if (!advancedRanking && source.contains("|")) source = source.substring(source.indexOf('|')+1);
						Double cnt = ranks.get(source);
						if (null == cnt) cnt = 0.0;
						cnt += srcCnt;
						bucketCnt += srcCnt;
						ranks.put(source,cnt);
					}
				}
			}
			else {
				for (String source : b.getSourceIDs()) {
					// assume uniform distribution if no detailed count is stored
					srcCnt = (double)b.getCount()/(double)b.getSourceIDs().size();
					Double cnt = ranks.get(source);
					if (null == cnt) cnt = 0.0;
					cnt += srcCnt;
					bucketCnt += srcCnt;
					ranks.put(source,cnt);
				}
			}
			//			assert 0.001 > Math.abs(bucketCnt - bucketCount(b)) : bucketCnt+" != "+bucketCount(b);
			assert 0.001 > Math.abs(bucketCnt/(double)joinDepth - b.getCount()) : bucketCnt/(double)joinDepth+" != "+b.getCount();
		}

		return ranks;
	}

	/**
	 * returns an ArrayList of sources, ordered by their rank after query estimation - the first one is the highest ranked
	 * 
	 * @param ranked
	 * @param joinDepth
	 * @return
	 */
	private ArrayList<String> getRankOrderedSources(SourceRankMap ranked, int joinDepth, boolean advancedRanking) {
		ArrayList<String> orderedSources = new ArrayList<String>();
		if (!advancedRanking) {
			// simply add the sources in descending order
			
			for(Entry<Double,Set<String>> rankSources: ranked.getRankSourceMap().descendingMap().entrySet()){
//				log.info("We have {} sources for rank {}", new Object[]{rankSources.getValue().size(),rankSources.getKey()});
				orderedSources.addAll(rankSources.getValue());
			}
//			for (Iterator<Set<String>> it = ranked.getRankSourceMap().values().iterator(); it.hasNext(); ) {
//				
//				
//			}
		}
		else {
			// first, we split ranking according to the different join levels
			ArrayList<String>[] advRanks = new ArrayList[joinDepth];
			// iterate descending
			for (Iterator<Set<String>> it = ranked.getRankSourceMap().values().iterator(); it.hasNext(); ) {
				Set<String> l = it.next();
				for(String source : l){
					// 	parse join level ...
					int level = new Integer(source.substring(0,source.indexOf('|'))) - 1;
					// ... and source name
					source = source.substring(source.indexOf('|')+1);
					if (null == advRanks[level]) advRanks[level] = new ArrayList<String>();
					// add the source to the ArrayList of the corresponding level - sorted access!
					advRanks[level].add(source);
				}
			}
			// advanced ranking means: add source from level 1, then 2, ..., then 1, ...
			int currLvl = -1;
			// count the number of already emptied ArrayLists
			int empty = 0;
			// while not all are empty
			while (advRanks.length > empty) {
				// determine the current join level to respect
				currLvl = (++currLvl) % joinDepth;
				// if the according ArrayList is already empty, continue with next join level
				if (advRanks[currLvl].isEmpty()) continue;
				// else, pop the next source
				String nextSource = advRanks[currLvl].remove(0);
				// update the number of empty ArrayLists
				if (advRanks[currLvl].isEmpty()) ++empty;
				// if this source is already inserted for another level, continue with the next join level
				if (!orderedSources.contains(nextSource)) orderedSources.add(nextSource);
			}
		}

		return orderedSources;
	}

	public void enableDebugMode(boolean enableDebug) {
		_debug = enableDebug;
	}
	
	public void enableJoinBucketDist(boolean enable,File outDir) {
		op.enableJoinBucketDist(enable);
		_jbDenabled= enable;
		jbdOut = outDir;
	}

	public void setInvBucketOperator(boolean b) {
		if(b) op = new InvListQTreeJoin();
		else{ op = new NestedLoopQTreeJoin();
		}
		
	}
}
