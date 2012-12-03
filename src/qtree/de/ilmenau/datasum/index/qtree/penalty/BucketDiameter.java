package de.ilmenau.datasum.index.qtree.penalty;

import de.ilmenau.datasum.index.Bucket;

/**
 * @author marcel
 * This Penalty Function Calculates the Bucket Diameter
 */
public class BucketDiameter extends QPenaltyFunction {

	/**
	 * Constructor
	 *
	 */
	public BucketDiameter() {
		super("BucketDiameter");
	}

	/*
	 * (non-Javadoc)
	 * @see smurfpdms.index.qtree.penalty.QPenaltyFunction#calculatePenalty(int[], int[], smurfpdms.index.Bucket)
	 */
	@Override
	public double calculatePenalty(int[] dimSpecMin, int[] dimSpecMax, Bucket b) {
		/*double area = 0.0;	
		
		for(int i=0; i<dimSpecMin.length; i++){		
			double scale = 1.0;			
			if (dimSpecMax[i] != dimSpecMin[i])
				scale = 1.0 / (double)(dimSpecMax[i] - dimSpecMin[i]);			
			area += scale * (double)(b.getUpperBoundaries()[i] - b.getLowerBoundaries()[i]);
		} 
		return Math.sqrt(area);*/

		double diameter = 0.0;
		
		for (int i=0;i<dimSpecMin.length; i++){
			double temp = (double)(b.getUpperBoundaries()[i] - b.getLowerBoundaries()[i]);
			double dimExtension = (double)(dimSpecMax[i] - dimSpecMin[i]);
			if (dimExtension != 0.0){
				temp = temp / dimExtension;	
			} 
			diameter += (double) temp * temp;
		}
		return Math.sqrt(diameter);
	}

}
