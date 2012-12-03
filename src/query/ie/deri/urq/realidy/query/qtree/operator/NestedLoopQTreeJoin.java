package ie.deri.urq.realidy.query.qtree.operator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.semanticweb.yars.stats.Count;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.ilmenau.datasum.index.AbstractIndex;
import de.ilmenau.datasum.index.Bucket;
import de.ilmenau.datasum.index.IntersectionInformation;
import de.ilmenau.datasum.index.JoinBucket;
import de.ilmenau.datasum.index.JoinSpace;
import de.ilmenau.datasum.index.QuerySpace;

public class NestedLoopQTreeJoin extends QTreeJoinOperator{
	private final static Logger log = LoggerFactory.getLogger(NestedLoopQTreeJoin.class);
	
	
	public  JoinSpace execute(AbstractIndex idx, JoinSpace left, int lpos,ArrayList<Bucket> right, int rpos, int joinLevel, boolean storeDetailCount) {
		int currDim = left.getNmbOfDimensions();
		final int newDim = currDim+3;
		JoinSpace result = new JoinSpace(newDim);

		
		int newBucketCnt = 0;

//		System.out.println("left");
//		for(Bucket b : left.getBuckets()){
//			System.out.println(b);
//		}
//		System.out.println("right");
//		for(Bucket b : right){
//			System.out.println(b);
//		}
		
		log.info(">>COMPUTE QTREE JOIN-{} lpos:{} lb:{} rpos:{} rb:{}",new Object[] {joinLevel,lpos,left.getBuckets().size(),rpos,right.size()});
		log.info(">>WORST CASE comparision {}",new Object[] {left.getBuckets().size()* right.size()});
		int count=0,count1=0;
		
		// determine the actual overlap with all intersection spaces from right
		for (Iterator<Bucket> rightIt = right.iterator(); rightIt.hasNext(); ) {
			Bucket currBucket = rightIt.next(); rightIt.remove();
			count1++;
//			System.err.println("\n  BUCKET: "+currBucket.getSourceIDs().size());
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
					lowerBoundaries[i] = idx.getDimSpecMin()[3%1];
					upperBoundaries[i] = idx.getDimSpecMax()[3%1];
				}
			}
			//			if(_debug)System.err.println("  "+joinLevel+" lower: "+Arrays.toString(lowerBoundaries));
			//			if(_debug)System.err.println("  "+joinLevel+" upper: "+Arrays.toString(upperBoundaries));
			QuerySpace querySpace = new QuerySpace(lowerBoundaries, upperBoundaries, false);
//			System.err.println("  QUERYSPACE low:"+lowerBoundaries.length+" up:"+upperBoundaries.length+":"+querySpace);
			// TODO: does this handle redundant buckets accordingly?
			ArrayList<IntersectionInformation> overlap = left.getAllBucketsInQuerySpace(querySpace);
//			System.err.println("  Overlap:"+overlap);
			
			if(_jbdEnable) _jbDist.add(overlap.size());
			for (Iterator<IntersectionInformation> infoIt = overlap.iterator(); infoIt.hasNext(); ) {
				count++;
				
				if(count%10000==0)log.info("compared {} buckets", count);
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
						rightLowerBoundaries[i] = idx.getDimSpecMin()[3%1];
						rightUpperBoundaries[i] = idx.getDimSpecMax()[3%1];
					}
				}
//				System.err.println("OverlapBucket"+info.getCorrespondingBucket().getSourceIDs().size());
				querySpace = new QuerySpace(rightLowerBoundaries, rightUpperBoundaries, false);
				//System.out.println("\t \t querySpace: "+querySpace.toString());
				log.debug("Queryspace "+querySpace);
				log.debug("currBucket "+currBucket);
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

					JoinSpace parentSpace =  null;
					insertBucket = new JoinBucket(lowerBoundaries,upperBoundaries,
							// it's a cross product between left and right sides
							(info.getCorrespondingBucket().getCount()*info.getIntersectionRatio() * currBucket.getCount()*rightOverlap.getIntersectionRatio()) / range,
							sourceIDMap,parentSpace);
//					sourceIDMap.clear();
//					sourceIDMap = null;
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
		log.info(" for join {}: {} comp operations ",new Object[]{joinLevel,count});
		
		return result;
		
	}

}
