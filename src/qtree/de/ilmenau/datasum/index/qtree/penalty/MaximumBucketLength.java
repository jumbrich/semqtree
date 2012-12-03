/* 
 *
 */
package de.ilmenau.datasum.index.qtree.penalty;

import de.ilmenau.datasum.index.Bucket;

/**
 * @author Matz
 * Version: $Id: MaximumBucketLength.java,v 1.1 2007-04-19 08:13:56 matz Exp $
 */
public class MaximumBucketLength extends QPenaltyFunction {

	/**
	 * Constructor
	 */
	public MaximumBucketLength() {
		super("MaximumBucketLength");
	}

	/* (non-Javadoc)
	 * @see smurfpdms.index.qtree.penalty.QPenaltyFunction#calculatePenalty(int[], int[], smurfpdms.index.Bucket)
	 */
	@Override
	public double calculatePenalty(int[] dimSpecMin, int[] dimSpecMax, Bucket b) {
		double area = 0.0;
		
		// 
		for(int i=0; i<dimSpecMin.length; i++){
			double scale = 1.0;
			
			if (dimSpecMax[i] != dimSpecMin[i])
				scale = 1.0 / (double)(dimSpecMax[i] - dimSpecMin[i]);
			
			double area1 = scale * (double)(b.getUpperBoundaries()[i] - b.getLowerBoundaries()[i]);
			
			if (area1 > area)
				area = area1;
		}
		
		return area;
	}

}
