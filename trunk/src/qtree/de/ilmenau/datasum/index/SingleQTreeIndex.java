/**
 * 
 */
package de.ilmenau.datasum.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import de.ilmenau.datasum.exception.QTreeException;
import de.ilmenau.datasum.index.qtree.QTree;
import de.ilmenau.datasum.index.qtree.QTreeBucket;
import de.ilmenau.datasum.index.update.UpdateBucket;
import de.ilmenau.datasum.util.bigmath.Space;

/**
 * @author khose
 *
 */
public class SingleQTreeIndex extends AbstractIndex{

	private static final long serialVersionUID = 1L;
	private final static Logger log = LoggerFactory.getLogger(SingleQTreeIndex.class);

	/**
	 * QTree parameters
	 */
	private final static  int dimensions = 3;

	private final int fanout;
	private final int buckets;

	public boolean storeDetailedCounts=true;
	public final QTree _sourcesQTree;
	
	
	
	/**
	 * /
	 * @param hasher **
	 * 
	 * @param hasher - for available hashing functions see{@link HashingFactory}
	 * @param buckets - number of buckets
	 * @param fanout - fanout value
	 * @param storeDetailedCounts
	 * @param dimMinValue
	 * @param dimMaxValue
	 * @param storeDetailedCounts
	 */
	public SingleQTreeIndex(String hasher, int maxBuckets, int fanout, int dimMinValue,
			int dimMaxValue, boolean storeDetailedCounts) {
		super(hasher,new int[]{dimMinValue,dimMinValue,dimMinValue},new int[]{dimMaxValue,dimMaxValue,dimMaxValue});
		this.buckets= maxBuckets;
		this.fanout=fanout;
		this.storeDetailedCounts = storeDetailedCounts;
		_sourcesQTree = new QTree(dimensions, fanout, buckets, dimSpecMin, dimSpecMax, "0", "1", storeDetailedCounts);	
	}



	/**
	 * 
	 * @param noOfStmts - the statement to insert into the QTree - stmt has to contain context information ->quad format
	 * @return - true if the statement was successfully added, false otherwise
	 * @throws Exception 
	 */
	protected void addStatment(final int[] hashCoordinates, String source) throws Exception {
		_sourcesQTree.insertDataItem(hashCoordinates, source);
	}


	/**
	 * NOT SURE IF THE METHOD STILL WORKS
	 * @return
	 */
	public int getNoOfSources(){
		if(!_sourcesQTree.storeDetailedCounts())
			return _sourcesQTree.getRoot().getSourceIDs().size();
		else
			return _sourcesQTree.getRoot().getSourceIDMap().size();
	}



	/**
	 * evaluates the query and returns only the relevant sources (ranked)
	 * @param bgps - 
	 * @param join
	 * @return - a {@link Collection} of the relevant sources ({@link String}), ordered by their rank  
	 * @throws QTreeException 
	 */
	public Collection<String> getRelevantSourcesForQuery(final int[][] bgpsHashCoordinates, final int[][] join) throws QTreeException {
		return evaluateQuery(bgpsHashCoordinates, join, true, false).getRelevantSourcesRanked();
	}

	public QueryResultEstimation evaluateQuery(final int[][] bgpsHashCoordinates, final int[][] join, boolean determineResultingBuckets, boolean useParentJoinSpace) throws QTreeException {
		long start = System.currentTimeMillis();
		if(_debug) System.err.println("Hashes of TriplePatterns:");for(int i =0; i < bgpsHashCoordinates.length;i++) System.err.println("  "+i+" > "+Arrays.toString(bgpsHashCoordinates[i]));

		QueryResultEstimation result = new QueryResultEstimation(bgpsHashCoordinates.length);
		try{
			//get queryspace for first bgp
			QuerySpace querySpace = getQuerySpace(bgpsHashCoordinates[0],false);
			if(_debug) System.err.println(" 0 QuerySpace: "+querySpace);
			// first, evaluate on QTree
			long start1 = System.currentTimeMillis();
			Vector<IntersectionInformation> leftResult = _sourcesQTree.getAllBucketsInQuerySpace(querySpace);
			//update the result object
			result.setBgpEvalTime(System.currentTimeMillis()-start1,0);
			result.setBgpEstSources(countSources(leftResult,storeDetailedCounts),0);
			if(_debug) System.err.println(" 0 srcs: "+countSources(leftResult,storeDetailedCounts));
			
			
			JoinSpace currResult = new JoinSpace(3);
			JoinSpace parentSpace = useParentJoinSpace? currResult : null;
			if(_debug) System.err.println(" 0 parentSpace: "+parentSpace);
			
			start1 = System.currentTimeMillis();
			currResult.add(prepareForQTreeJoin(leftResult,0,parentSpace));

			if (determineResultingBuckets) result.setBgpResultingBuckets(currResult.bucketCount(),0);
			result.setBgpQTreeBuildTime(System.currentTimeMillis()-start1,0);
			if(_debug) System.err.println(" 0 buckets: "+currResult.bucketCount());
			

			leftResult = null;

			// if the left result is empty, the whole join is empty
			if (null != currResult) {
				for (int j = 1; j < bgpsHashCoordinates.length; j++) {
					// first, evaluate on QTree
					QuerySpace rightQuerySpace = getQuerySpace(bgpsHashCoordinates[j],false);
					start1 = System.currentTimeMillis();
					if(_debug) System.err.println(" "+j+" rightQuerySpace "+rightQuerySpace);
					Vector<IntersectionInformation> rightResult = _sourcesQTree.getAllBucketsInQuerySpace(rightQuerySpace);

					result.setBgpEvalTime(System.currentTimeMillis()-start1,j);
					result.setBgpEstSources(countSources(rightResult,storeDetailedCounts),j);
					if(_debug) System.err.println(" "+j+" srcs: "+countSources(rightResult,storeDetailedCounts));
					// if the right result is empty, the whole join is empty
					if (0 == rightResult.size()) currResult = null;

					// if an intermediate result is empty, the whole join is empty
					if (null == currResult) break;

					parentSpace = useParentJoinSpace? currResult : null;
					if(_debug) System.err.println(" "+j+" parentSpace: "+parentSpace);
					start1 = System.currentTimeMillis();

					if(_debug) System.err.println(" "+j+" buckets: "+prepareForQTreeJoin(rightResult,j,parentSpace).size());
					currResult = computeQTreeJoin(currResult, join[j-1][0], prepareForQTreeJoin(rightResult,j,parentSpace), join[j-1][1], j, useParentJoinSpace);
					if(_debug) System.err.println(" after "+j+" buckets: "+currResult.bucketCount());
					if (determineResultingBuckets){ result.setJoinResultingBuckets(currResult.bucketCount(), j-1);
						result.setBgpResultingBuckets(prepareForQTreeJoin(rightResult,j,parentSpace).size(),j);}
					rightResult = null;
					
					result.setJoinEvalTime(System.currentTimeMillis()-start1,j-1);
				}
			}
			result.setTotalQueryTime(System.currentTimeMillis()-start);
			if(_debug)System.err.println("Source estimation completed in "+(System.currentTimeMillis()-start)+" ms");
			start1 = System.currentTimeMillis();
			
			TreeMap<Double,String> ranked = rankSources(currResult, join.length+1,false);
			
			result.setRelevantSourcesRanked(getRankOrderedSources(ranked, join.length+1,false));
			result.setRankTime(System.currentTimeMillis()-start1);

			ranked = null;
			currResult = null;
		}catch(NullPointerException e){
			throw new QTreeException(e.getClass().getSimpleName()+" msg: no results available",e);
		}
		return result;
	}

	/**
	 * 
	 * @param q
	 * @param hasNoRestrictions
	 * @return
	 */
	private QuerySpace getQuerySpace(int[] hashCoordinates, boolean hasNoRestrictions) {
		int[] lowerBoundaries = Arrays.copyOf(hashCoordinates,hashCoordinates.length);
		int[] upperBoundaries = Arrays.copyOf(hashCoordinates,hashCoordinates.length);
		for (int i=0;i<hashCoordinates.length;i++) {
			if (hashCoordinates[i]== VARIABLE) {
				// variables
				lowerBoundaries[i] = dimSpecMin[i];
				upperBoundaries[i] = dimSpecMax[i];
			}
		}

		return new QuerySpace(lowerBoundaries, upperBoundaries, hasNoRestrictions);
	}

	private int countSources(Vector<IntersectionInformation> v, boolean storeDetailedCounts) {
		Set<String> s = new HashSet<String>();
		for (IntersectionInformation i : v) {
			if (!storeDetailedCounts) s.addAll(i.getCorrespondingBucket().getSourceIDs());
			else s.addAll(i.getCorrespondingBucket().getSourceIDMap().keySet());
		}
		return s.size();
	}



	private JoinSpace computeQTreeJoin(JoinSpace left, int lpos,ArrayList<Bucket> right, int rpos, int joinLevel, boolean useParentJoinSpace) {
		int currDim = left.getNmbOfDimensions();
		final int newDim = currDim+3;
		JoinSpace result = new JoinSpace(newDim);
		
		int[] lowerBoundaries = new int[currDim];
		int[] upperBoundaries = new int[currDim];
		
		int newBucketCnt = 0;
		// determine the actual overlap with all intersection spaces from right
		for (Iterator<Bucket> rightIt = right.iterator(); rightIt.hasNext(); ) {
			Bucket currBucket = rightIt.next(); rightIt.remove();
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
					lowerBoundaries[i] = this.dimSpecMin[3%1];
					upperBoundaries[i] = this.dimSpecMax[3%1];
				}
			}
//			if(_debug)System.err.println("  "+joinLevel+" lower: "+Arrays.toString(lowerBoundaries));
//			if(_debug)System.err.println("  "+joinLevel+" upper: "+Arrays.toString(upperBoundaries));
			QuerySpace querySpace = new QuerySpace(lowerBoundaries, upperBoundaries, false);
			if(_debug)System.err.println("  "+joinLevel+" querySpace: "+querySpace);
			// TODO: does this handle redundant buckets accordingly?
			ArrayList<IntersectionInformation> overlap = left.getAllBucketsInQuerySpace(querySpace);
			if(_debug)System.err.println("  "+joinLevel+" buckets in query space: "+overlap.size());
			
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
						rightLowerBoundaries[i] = this.dimSpecMin[3%1];
						rightUpperBoundaries[i] = this.dimSpecMax[3%1];
					}
				}
				querySpace = new QuerySpace(rightLowerBoundaries, rightUpperBoundaries, false);
				//System.out.println("\t \t querySpace: "+querySpace.toString());
				IntersectionInformation rightOverlap = JoinSpace.computeIntersectionInformation(currBucket,querySpace);
				JoinBucket insertBucket; 
				double range = info.getCorrespondingBucket().getUpperBoundaries()[lpos] - info.getCorrespondingBucket().getLowerBoundaries()[lpos];
				if ((currBucket.getUpperBoundaries()[rpos] - currBucket.getLowerBoundaries()[rpos]) > range) range = currBucket.getUpperBoundaries()[rpos] - currBucket.getLowerBoundaries()[rpos];
				if (0.0 == range) range = 1.0;
				if (storeDetailedCounts) {
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
			currBucket = null;
			overlap = null;
		}
		left.clear();
		right.clear();
		right = null;
		left = null;

		//    	if (result.getAllBuckets().size() != newBucketCnt) {
		//    		logger.warn("OnDiskOne4AllQTreeIndex.computeQTreeJoin: Unexpected number of buckets in QTree: "+result.getAllBuckets().size()+" (expected: "+newBucketCnt+")");
		//    	}
		if (buckets < newBucketCnt) {
			log.info("Exceeded original number of buckets in QTree: "+newBucketCnt+" (original: "+buckets+")");
		}
		return result;
	}

	private ArrayList<Bucket> prepareForQTreeJoin(Vector<IntersectionInformation> spaces, int joinLevel, JoinSpace joinSpace)  {
		if (0 == spaces.size()) return null;
		ArrayList<Bucket> buckets = new ArrayList<Bucket>();

		// insert the spaces from left into a temporary QTree in order to determine the overlap in the join dimension
		for (IntersectionInformation interSec : spaces) {
			//    		Bucket currBucket = interSec.getCorrespondingBucket();
			Bucket orgBucket = interSec.getCorrespondingBucket();
			Bucket currBucket;
			if (storeDetailedCounts) {
				double bucketCnt = 0.0;
				HashMap<String,Double> sourceIDMap = new HashMap<String, Double>();
				for (Map.Entry<String,Double> e : orgBucket.getSourceIDMap().entrySet()) {
					double newCnt = e.getValue()*interSec.getIntersectionRatio();
					bucketCnt += newCnt;
					//	    			System.out.println("0:"+newCnt);
					// add the join level to the source name - thus, we maintain counts for the different levels separately
					//	    			sourceIDMap.put((joinLevel+1)+"|"+e.getKey(),newCnt);
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
	 * builds a QTree on the basis of some IntersectionInformation
	 * 
	 * @param spaces
	 * @return
	 */
	private QTree buildJoinQtree(Vector<IntersectionInformation> spaces, int joinLevel, boolean unlimitedBuckets) throws Exception {
		if (0 == spaces.size()) return null;

		int currDim = spaces.firstElement().getCorrespondingBucket().getLowerBoundaries().length;
		int[] tmpdimSpecMin = new int[currDim];
		int[] tmpdimSpecMax = new int[currDim];

		// prepare the QTree
		// TODO: should be made dynamic (maxF, maxB)
		for (int i=0; i<currDim; ++i) {
			tmpdimSpecMin[i] = this.dimSpecMin[i%3];
			tmpdimSpecMax[i] = this.dimSpecMin[i%3];
		}

		Vector<UpdateBucket> newBuckets = new Vector<UpdateBucket>();
		// insert the spaces from left into a temporary QTree in order to determine the overlap in the join dimension
		//    	for (IntersectionInformation interSec : spaces) {
		for (Iterator<IntersectionInformation> it = spaces.iterator(); it.hasNext(); ) {
			IntersectionInformation interSec = it.next();
			it.remove();
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
				assert 0.001 > Math.abs(bucketCnt - sumSourceCounts(currBucket)*interSec.getIntersectionRatio()) : bucketCnt+" != "+sumSourceCounts(currBucket)*interSec.getIntersectionRatio();
				insertBucket.setSourceIDMap(sourceIDMap);
			}
			else {
				insertBucket.setSourceIDs(currBucket.getSourceIDs());
			}
			newBuckets.add(insertBucket);
		}
		spaces = null;
		//generate a identical QTree as the parent one
		QTree result;
		// we set the maximal bucket number so that no merging should occur (add 2 "safety" buckets)
		if (unlimitedBuckets) result = new QTree(currDim,fanout,newBuckets.size()+2,dimSpecMin,dimSpecMax,"0","1",storeDetailedCounts);
		else result = new QTree(currDim,fanout,buckets,dimSpecMin,dimSpecMax,"0","1",storeDetailedCounts);
		//    	for (UpdateBucket b : newBuckets) result.insertBucket(b,false);
		int newBucketCnt = 0;
		for (Iterator<UpdateBucket> it = newBuckets.iterator(); it.hasNext(); ) {
			result.insertBucket(it.next(),false);
			it.remove();
			++newBucketCnt;
		}
		newBuckets = null;
		//    	if (result.getAllBuckets().size() != newBuckets.size()) {
		//    		logger.warn("OnDiskOne4AllQTreeIndex.buildJoinQtree: Unexpected number of buckets in QTree: "+result.getAllBuckets().size()+" (expected: "+newBuckets.size()+")");
		//    	}
		if (buckets < newBucketCnt) {
			log.info("Exceeded original number of buckets in QTree: "+newBucketCnt+" (original: "+buckets+")");
		}
		return result;
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
	private TreeMap<Double,String> rankSources(QTree result, int joinDepth, boolean advancedRanking) {
		Map<String,Double> ranks = new HashMap<String,Double>();
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

		// now, switch key and values - thus, we use the sorting of the TreeMap to do the actual ranking
		TreeMap<Double,String> ranked = new TreeMap<Double, String>();
		for (Map.Entry<String,Double> sources : ranks.entrySet()) {
			String sourceName = sources.getKey();
			double cnt = sources.getValue();
			// there may be equally ranked sources - just add a minor value for them
			// TODO: I know, it's not perfect...! had a much smaller value in the beginning, resulted in an endless loop for very huge counts (+0.000001 has no effect!?!)
			while (ranked.containsKey(cnt)) cnt += 0.001;
			ranked.put(cnt,sourceName);
		}
		//		System.out.println(ranked);
		return ranked;
	}

	private TreeMap<Double,String> rankSources(JoinSpace result, int joinDepth, boolean advancedRanking) {
		long start = System.currentTimeMillis();
		Map<String,Double> ranks = new HashMap<String,Double>();
		// first, collect the rank values for each source
		if(_debug)System.err.println("Ranking the sources from "+result.bucketCount()+" buckets");
		for (Bucket b : result.getBuckets()) {
			if(_debug)System.err.print(b);
			// only for the assert check
			double bucketCnt = 0.0;
			double srcCnt;
			if (storeDetailedCounts) {
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
					//		    for (String source : b.getSourceIDMap().keySet()) {
					//			srcCnt = b.getSourceIDMap().get(source);
					//			// remove the join level from the source name - thus, we accumulate over all join levels
					//			if (!advancedRanking && source.contains("|")) source = source.substring(source.indexOf('|')+1);
					//			Double cnt = ranks.get(source);
					//			if (null == cnt) cnt = 0.0;
					//			cnt += srcCnt;
					//			bucketCnt += srcCnt;
					//			ranks.put(source,cnt);
					//		    }
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
			if(_debug)System.err.println("done");
		}

		// now, switch key and values - thus, we use the sorting of the TreeMap to do the actual ranking
		if(_debug)System.err.println("Switching key value pairs (size "+ranks.size()+")");
		TreeMap<Double,String> ranked = new TreeMap<Double, String>();
		double cnt,tmp, diff;
		
		for (Map.Entry<String,Double> sources : ranks.entrySet()) {
			diff=0.0001;
			String sourceName = sources.getKey();
			cnt = sources.getValue();
			
			// there may be equally ranked sources - just add a minor value for them
			// TODO: I know, it's not perfect...! had a much smaller value in the beginning, resulted in an endless loop for very huge counts (+0.000001 has no effect!?!)
//			System.out.println("Do the loop");
			
			while (ranked.containsKey(cnt)) {
				tmp = diff+cnt;
				if(tmp==cnt) diff=diff*1.5;
				cnt=tmp;
//				System.out.println(cnt);
			}
//			System.out.println("put "+cnt+" for "+sourceName);
			ranked.put(cnt,sourceName);
		}
		if(_debug)System.err.println("Ranking of sources done in "+(System.currentTimeMillis()-start));
		//		System.out.println(ranked);
		return ranked;
	}

	/**
	 * returns an ArrayList of sources, ordered by their rank after query estimation - the first one is the highest ranked
	 * 
	 * @param ranked
	 * @param joinDepth
	 * @return
	 */
	private ArrayList<String> getRankOrderedSources(TreeMap<Double,String> ranked, int joinDepth, boolean advancedRanking) {
		ArrayList<String> orderedSources = new ArrayList<String>();
		if (!advancedRanking) {
			// simply add the sources in descending order
			for (Iterator<String> it = ranked.descendingMap().values().iterator(); it.hasNext(); ) {
				orderedSources.add(it.next());
			}
		}
		else {
			// first, we split ranking according to the different join levels
			ArrayList<String>[] advRanks = new ArrayList[joinDepth];
			// iterate descending
			for (Iterator<String> it = ranked.descendingMap().values().iterator(); it.hasNext(); ) {
				String source = it.next();
				// parse join level ...
				int level = new Integer(source.substring(0,source.indexOf('|'))) - 1;
				// ... and source name
				source = source.substring(source.indexOf('|')+1);
				if (null == advRanks[level]) advRanks[level] = new ArrayList<String>();
				// add the source to the ArrayList of the corresponding level - sorted access!
				advRanks[level].add(source);
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

	public String info(){
		StringBuilder sb = new StringBuilder();
		sb.append("  buckets:  ").append(_sourcesQTree.getCurrBuckets()).append("/").append(buckets).append("\n");
		sb.append("  fanout:   ").append(fanout).append("\n");
		sb.append("  min:      ").append(dimSpecMin[0]).append("\n");
		sb.append("  max:      ").append(dimSpecMax[0]).append("\n");
		sb.append("  detailed: ").append(storeDetailedCounts).append("\n");
		sb.append("  ------------------\n");
		sb.append("  stmts:    ").append(noOfStmts).append("\n");
		sb.append("  srcs:     ").append(getNoOfSources()).append("\n");
		return sb.toString();
	}



	@Override
	public Vector<IntersectionInformation> getAllBucketsInQuerySpace(
			QuerySpace querySpace) {
		return _sourcesQTree.getAllBucketsInQuerySpace(querySpace);
	}



	@Override
	public boolean getStoreDetailedCount() {
		return _sourcesQTree.storeDetailedCounts();
	}



	@Override
	protected String versionType() {
		return "qtree";
	}
}