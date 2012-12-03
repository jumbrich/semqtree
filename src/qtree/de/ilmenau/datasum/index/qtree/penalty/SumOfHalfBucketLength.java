/**
 * 
 */
package de.ilmenau.datasum.index.qtree.penalty;

import de.ilmenau.datasum.index.Bucket;

/**
 * @author marcel
 *
 */
public class SumOfHalfBucketLength extends QPenaltyFunction {

	/**
	 * @param id
	 */
	public SumOfHalfBucketLength() {
		super("SumOfHalfBucketLength");
	}

	/* (non-Javadoc)
	 * @see smurfpdms.index.qtree.penalty.QPenaltyFunction#calculatePenalty(int[], int[], smurfpdms.index.Bucket)
	 */
	@Override
	public double calculatePenalty(int[] dimSpecMin, int[] dimSpecMax, Bucket b) {
		double area = 0.0;
		
		for(int i=0; i<b.getLowerBoundaries().length; i++){		
			double scale = 1.0;			
			if (dimSpecMax[i] != dimSpecMin[i])
				scale = 1.0 / (double)(dimSpecMax[i] - dimSpecMin[i]);			
			area += scale * (double)(b.getUpperBoundaries()[i] - b.getLowerBoundaries()[i]); //*0.5
		} 
		return area;		
	}

}
