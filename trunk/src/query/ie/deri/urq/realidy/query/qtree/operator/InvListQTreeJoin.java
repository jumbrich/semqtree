package ie.deri.urq.realidy.query.qtree.operator;

import ie.deri.urq.realidy.query.qtree.InvertedBucketIndex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.semanticweb.yars.stats.Count;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.ilmenau.datasum.index.AbstractIndex;
import de.ilmenau.datasum.index.Bucket;
import de.ilmenau.datasum.index.IntersectionInformation;
import de.ilmenau.datasum.index.JoinBucket;
import de.ilmenau.datasum.index.JoinSpace;
import de.ilmenau.datasum.index.QuerySpace;

public class InvListQTreeJoin extends QTreeJoinOperator{
	private final static Logger log = LoggerFactory.getLogger(InvListQTreeJoin.class);
	
	public  JoinSpace execute(AbstractIndex idx, JoinSpace left, int lpos,ArrayList<Bucket> right, int rpos, int joinLevel, boolean storeDetailCount) {
		int currDim = left.getNmbOfDimensions();
		final int newDim = currDim+3;
		JoinSpace result = new JoinSpace(newDim);

		
		log.info(">>COMPUTE QTREE JOIN-{} lpos:{} lb:{} rpos:{} rb:{}",new Object[] {joinLevel,lpos,left.getBuckets().size(),rpos,right.size()});
		log.info(">>WORST CASE comparision {}",new Object[] {left.getBuckets().size()* right.size()});
		int count=0;

		//build the inverted index from the right
		InvertedBucketIndex invIdx = new InvertedBucketIndex(right,rpos,idx.getDimSpecMax()[rpos]);
		//now for each bucket in left, compute the intersection
		for(Bucket bleft: left.getBuckets()){
			Set<Bucket> bl= invIdx.getOverlap(bleft.getLowerBoundaries()[lpos],bleft.getUpperBoundaries()[lpos]);
			if(_jbdEnable) _jbDist.add(bl.size());
			
			for(Bucket bright: bl){
				count++;
				result = handleIntersection(result,left, bleft, bright, lpos, rpos,idx.getDimSpecMin()[3%1],idx.getDimSpecMax()[3%1],storeDetailCount,joinLevel);
			}
		}
		log.info("#Operations:"+count);
		return result;
	}

	private JoinSpace handleIntersection(JoinSpace result, JoinSpace left, Bucket bleft, Bucket bright, int lpos, int rpos, int min, int max,boolean storeDetailCount, int joinLevel) {
		int currDim = left.getNmbOfDimensions();
		final int newDim = currDim+3;

		int[] lowerBoundaries = new int[currDim];
		int[] upperBoundaries = new int[currDim];
		for (int i=0; i<currDim; ++i) {
			// only restrict the join dimension
			if (i == lpos) {
				// use the boundaries of the right tree to limit the buckets of the left tree
				// should be the same vice versa
				lowerBoundaries[i] = bright.getLowerBoundaries()[rpos];
				upperBoundaries[i] = bright.getUpperBoundaries()[rpos];
			}
			else {
				// all other dimensions are united without restrictions
				lowerBoundaries[i] = min;
				upperBoundaries[i] = max;
			}
		}
		QuerySpace querySpace = new QuerySpace(lowerBoundaries, upperBoundaries, false);

		IntersectionInformation overlap=JoinSpace.computeIntersectionInformation(bleft, querySpace);
		//				
		lowerBoundaries = new int[newDim];
		upperBoundaries = new int[newDim];
		// set the first dimensions from the overlap of the left side
		for (int i=0; i<currDim; ++i) {
			lowerBoundaries[i] = overlap.getIntersection().getLowerBoundaries()[i];
			upperBoundaries[i] = overlap.getIntersection().getUpperBoundaries()[i];
		}
		// set the new dimensions from the right side
		for (int i=currDim; i<newDim; ++i) {
			// the join dimensions are equal
			if (i == currDim+rpos) {
				lowerBoundaries[i] = lowerBoundaries[lpos];
				upperBoundaries[i] = upperBoundaries[lpos];
			}
			else {
				lowerBoundaries[i] = bright.getLowerBoundaries()[i-currDim];
				upperBoundaries[i] = bright.getUpperBoundaries()[i-currDim];
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
				rightLowerBoundaries[i] = overlap.getIntersection().getLowerBoundaries()[lpos];
				rightUpperBoundaries[i] = overlap.getIntersection().getUpperBoundaries()[lpos];
			}
			else {
				// all other dimensions are united without restrictions
				rightLowerBoundaries[i] = min;
				rightUpperBoundaries[i] = max;
			}
		}
		querySpace = new QuerySpace(rightLowerBoundaries, rightUpperBoundaries, false);

		IntersectionInformation rightOverlap = JoinSpace.computeIntersectionInformation(bright,querySpace);
		JoinBucket insertBucket; 
		double range = overlap.getCorrespondingBucket().getUpperBoundaries()[lpos] - overlap.getCorrespondingBucket().getLowerBoundaries()[lpos];
		if ((bright.getUpperBoundaries()[rpos] - bright.getLowerBoundaries()[rpos]) > range) range = bright.getUpperBoundaries()[rpos] - bright.getLowerBoundaries()[rpos];
		if (0.0 == range) range = 1.0;



		if (storeDetailCount) {
			// only for the assert check
			double bucketCnt = 0.0;
			HashMap<String,Double> sourceIDMap = new HashMap<String, Double>();
			// set sources and counts from the current buckets of the left input QTree
			if (overlap.getCorrespondingBucket() instanceof JoinBucket && null == overlap.getCorrespondingBucket().getSourceIDMap()) {
				//		    			System.err.println("1: Yeappa!!!");
				JoinBucket joinBucket = (JoinBucket)overlap.getCorrespondingBucket();
				for (int i = 0; i < left.getNrOfSources(); ++i) {
					double cnt = joinBucket.getSourceCnt(i);
					if (-1 == cnt) break;
					if (0.0 < cnt) {
						// it's a cross product between left and right sides
						double newCnt = (cnt*overlap.getIntersectionRatio() * bright.getCount()*rightOverlap.getIntersectionRatio()) / range;
						bucketCnt += newCnt;
						sourceIDMap.put(left.getSource(i),newCnt);
					}
				}
			}
			else {
				for (Iterator<Map.Entry<String,Double>> entryIt = overlap.getCorrespondingBucket().getSourceIDMap().entrySet().iterator(); entryIt.hasNext(); ) {
					Map.Entry<String,Double> e = entryIt.next();
					// it's a cross product between left and right sides
					//		    			double newCnt = e.getValue()*info.getIntersectionRatio() * bucketCount(currBucket)*rightOverlap.getIntersectionRatio();
					double newCnt = (e.getValue()*overlap.getIntersectionRatio() * bright.getCount()*rightOverlap.getIntersectionRatio()) / range;
					//		    			System.out.println("1:"+e.getValue()+"*"+info.getIntersectionRatio()+" * "+currBucket.getCount()+"*"+rightOverlap.getIntersectionRatio());

					// divide by 2 because it's only one side of the join
					// this is one approach: 30 result triples, 3 sources -> assign 10 to each source...
					//		    			newCnt /= 2.0;
					// ...actually, the accumulated one works better: 30 result triples, 3 sources -> assign 30 to each source (i.e., sum(assignment)=#restriples*#joinlevel)

					//		    			System.out.println("newCnt: "+newCnt);
					bucketCnt += newCnt;
					sourceIDMap.put(e.getKey(),newCnt);
				}

			}
			// iterate over the buckets from the right input QTree and set sources and counts
			if (bright instanceof JoinBucket && null == bright.getSourceIDMap()) {
				//		    			System.err.println("2: Yeappa!!!");
				JoinBucket joinBucket = (JoinBucket)bright;
				for (int i = 0; i < left.getNrOfSources(); ++i) {
					double cnt = joinBucket.getSourceCnt(i);
					if (-1 == cnt) break;
					if (0.0 < cnt) {
						double newCnt = (overlap.getCorrespondingBucket().getCount()*overlap.getIntersectionRatio() * cnt*rightOverlap.getIntersectionRatio()) / range;
						bucketCnt += newCnt;
						String source = left.getSource(i);
						if (sourceIDMap.containsKey(source)) newCnt += sourceIDMap.get(source);
						sourceIDMap.put(source,newCnt);
					}
				}
			}
			else {
				for (Iterator<Map.Entry<String,Double>> entryIt = bright.getSourceIDMap().entrySet().iterator(); entryIt.hasNext(); ) {
					Map.Entry<String,Double> e = entryIt.next();
					//		    			if (!infoIt.hasNext()) entryIt.remove();
					// we have to divide by joinLevel, cause we accumulate (i.e., 30 triples, 3 sources -> bucketCount()=90)!
					//		    			double newCnt = bucketCount(info.getCorrespondingBucket())/(joinLevel)*info.getIntersectionRatio() * e.getValue()*rightOverlap.getIntersectionRatio();
					double newCnt = (overlap.getCorrespondingBucket().getCount()*overlap.getIntersectionRatio() * e.getValue()*rightOverlap.getIntersectionRatio()) / range;
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
			assert 0.001 > Math.abs(bucketCnt/(double)(joinLevel+1) - (overlap.getCorrespondingBucket().getCount()*overlap.getIntersectionRatio() * bright.getCount()*rightOverlap.getIntersectionRatio()) / range) : 
				bucketCnt/(double)(joinLevel+1)+" != "+(overlap.getCorrespondingBucket().getCount()*overlap.getIntersectionRatio() * bright.getCount()*rightOverlap.getIntersectionRatio()) / range;

			JoinSpace parentSpace =  null;
			insertBucket = new JoinBucket(lowerBoundaries,upperBoundaries,
					// it's a cross product between left and right sides
					(overlap.getCorrespondingBucket().getCount()*overlap.getIntersectionRatio() * bright.getCount()*rightOverlap.getIntersectionRatio()) / range,
					sourceIDMap,parentSpace);
			sourceIDMap.clear();
			sourceIDMap = null;
		}
		else {
			HashSet<String> sourceIds = new HashSet<String>();
			sourceIds.addAll(overlap.getCorrespondingBucket().getSourceIDs());
			sourceIds.addAll(bright.getSourceIDs());
			insertBucket = new JoinBucket(lowerBoundaries,upperBoundaries,
					// it's a cross product between left and right sides
					(overlap.getCorrespondingBucket().getCount()*overlap.getIntersectionRatio() * bright.getCount()*rightOverlap.getIntersectionRatio()) / range,
					sourceIds);
		}
		overlap = null;
		result.add(insertBucket);
		return result;

	}
}
