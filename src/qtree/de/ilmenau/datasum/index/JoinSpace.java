package de.ilmenau.datasum.index;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import de.ilmenau.datasum.util.bigmath.BigMathHelper;
import de.ilmenau.datasum.util.bigmath.BigUInt;
import de.ilmenau.datasum.util.bigmath.DifferentByteSizeException;
import de.ilmenau.datasum.util.bigmath.Space;

public class JoinSpace {
	private static final long serialVersionUID = 1L;
	private int dim;
	private ArrayList<Bucket> buckets;
	private ArrayList<String> sourceNames;
	
	public JoinSpace(int dim) {
		this.dim = dim;
		buckets = new ArrayList<Bucket>();
		sourceNames = new ArrayList<String>();
	}
	
	public List<String> getSources(){
	    return sourceNames;
	}
	
	public JoinSpace (Vector<IntersectionInformation> data, int dim) {
		this(dim);
		for (IntersectionInformation i : data) {
			buckets.add(i.getCorrespondingBucket());
		}
	}
	
	public JoinSpace (ArrayList<Bucket> data, int dim) {
		this(dim);
		buckets.addAll(data);
	}
	
	public void clear() {
		sourceNames.clear();
		sourceNames = null;
		buckets.clear();
		buckets = null;
	}

	public void add(ArrayList<Bucket> data) {
	    if(data!=null)
		buckets.addAll(data);
	}
	
	public int addSource(String source) {
		int idx = sourceNames.indexOf(source);
		if (-1 == idx) {
			sourceNames.add(source);
			idx = sourceNames.size()-1;
		}
		return idx;
	}
	
	public int getNrOfSources() {
		return sourceNames.size();
	}
	
	public String getSource(int idx) {
		return sourceNames.get(idx);
	}
	
	public void add(Bucket bucket) {
		buckets.add(bucket);
	}
	
	public int bucketCount() {
		return buckets.size();
	}
	
	public ArrayList<Bucket> getBuckets() {
		return buckets;
	}
	
	public ArrayList<IntersectionInformation> getAllBucketsInQuerySpace(QuerySpace querySpace) {
		ArrayList<IntersectionInformation> result = new ArrayList<IntersectionInformation>();
		for (Bucket currentBucket : buckets){
			Space intersection = currentBucket.intersect(querySpace);
			if (intersection != null) {
				// add to result list
				result.add(computeIntersectionInformation(currentBucket, querySpace));
			}
		}
		
		return result;
	}
	
	public static IntersectionInformation computeIntersectionInformation(Bucket bucket, QuerySpace qs) {
		// compute the intersection space
		Space intersection = bucket.intersect(qs);
		// --> calculate the space needed for the bucket capacity (BigUInt)
		int estimatedByteSizeNeeded = BigMathHelper.getByteSizeToUse(
			BigMathHelper.getEstimatedCapacityBitSize(bucket)
		);
		BigUInt bucketCapacity = new BigUInt(estimatedByteSizeNeeded);
		BigUInt intersectionCapacity = new BigUInt(estimatedByteSizeNeeded);
		// --> calculate the capacities
		bucket.calculateCapacity(bucketCapacity);
		intersection.calculateCapacity(intersectionCapacity);
		// --> at last calculate the ratio (intersection capacity / bucket capacity)
		try {
			// 'FIXPOINT_RANGE_EXTENSION * 8' bits dual floating points
			intersectionCapacity.shiftLeft(BigMathHelper.FIXPOINT_RANGE_EXTENSION * 8);
			intersectionCapacity.div(bucketCapacity);
		} catch (DifferentByteSizeException e) {
			// could not happen
		}

		return new IntersectionInformation(
			bucket, intersection,
			(intersectionCapacity.getLower63Bits()
				/ Math.pow(2.0, BigMathHelper.FIXPOINT_RANGE_EXTENSION * 8))
		);
	}
	
	public int getNmbOfDimensions() {
		return dim;
	}

}
