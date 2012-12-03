/**
 * 
 */
package de.ilmenau.datasum.index.qtree.penalty;

import de.ilmenau.datasum.index.Bucket;

/**
 * @author marcel
 *
 */
public class BucketVolume extends QPenaltyFunction {

	/**
	 */
	public BucketVolume() {
		super("BucketVolume");
	}

	/* (non-Javadoc)
	 * @see smurfpdms.index.qtree.penalty.QPenaltyFunction#calculatePenalty(int[], int[], smurfpdms.index.Bucket)
	 */
	@Override
	public double calculatePenalty(int[] dimSpecMin, int[] dimSpecMax, Bucket b) {
		double volume=1;
		int nmbOfZeros = 0;
		for (int i=0;i<b.getLowerBoundaries().length;i++){
			if (b.getUpperBoundaries()[i] != b.getLowerBoundaries()[i]){
				volume *= b.getUpperBoundaries()[i] - b.getLowerBoundaries()[i];
			} else {
				nmbOfZeros++;
			}
			//volume *= ((double) b.getUpperBoundaries()[i] - b.getLowerBoundaries()[i] + 1) / (double)(dimSpecMax[i] - dimSpecMin[i]);
		}
		if (nmbOfZeros==dimSpecMax.length){
			volume = 0;
		}
		
		/*double area=1;
		double lenSum=0;
		double baseNorm = dimSpecMax[0] - dimSpecMin[0];     // Base Norm Extension
		for (int i=0;i<b.getLowerBoundaries().length;i++){
			double normFactor = (dimSpecMax[i] - dimSpecMin[i]) / baseNorm;
			double diff = b.getUpperBoundaries()[i] - b.getLowerBoundaries()[i];
			if (diff != 0){
				area = area * (diff / normFactor);
				lenSum += diff;
			}
			else {
				lenSum += 1;
			}
		}
		// Volume only makes some ugly wide Buckets into one dimension,
		// so we adjust this by adding a second element to get more rectangle buckets
		double lenProp = lenSum / b.getLowerBoundaries().length;
		return (area*lenProp);*/
		return volume;
	}

}
