/*
 * 
 */
package de.ilmenau.datasum.util.bigmath;


/**
 * This class contains various helper methods in conjunction with {@link BigUInt}.
 * 
 * @author Christian Lemke
 * @version $Id$
 */
public final class BigMathHelper {

	/** number of bytes necessary to store 16 fix points */
	public final static long FIXPOINT_RANGE_EXTENSION = 7L;


	/**
	 * method that calculates the logarithm to the basis 2 (rounded up) of the given value.
	 * (only the lower 63 bits are used -> so it is unsigned)
	 * 
	 * @param value the value
	 * @param floorLogarithm the logarithm to the basis 2 (rounded down)
	 * @return the ceil logarithm
	 */
	public static int ceilLogarithm(long value, int floorLogarithm) {
		value &= 0x7fffffffffffffffL;
		int firstBitSetPos = 0;

		for (int i = 0; i < 64; i++) {
			if ((value & 0x01) == 0) {
				value >>>= 1;
			} else {
				firstBitSetPos = i;
				break;
			}
		}
		if (floorLogarithm > firstBitSetPos) {
			return floorLogarithm + 1;
		}
		return floorLogarithm;
	}

	/**
	 * method that calculates the logarithm to the basis 2 (rounded down) of the given value.
	 * (only the lower 63 bits are used -> so it is unsigned)
	 * 
	 * @param value the value
	 * @return the floor logarithm
	 */
	public static int floorLogarithm(long value) {
		value &= 0x7fffffffffffffffL;
		for (int i = 63; i >= 0; i--) {
			if ((value & 0x8000000000000000L) == 0) {
				value <<= 1;
			} else {
				return i;
			}
		}
		return 0;
	}

	/**
	 * method that returns the amount of bytes necessary to store a number with the given bits count.
	 * (used to create a {@link BigUInt} that is big enough)
	 * 
	 * @param minBitSizeNecessary how many bits to store
	 * @return amount of needed bytes
	 */
	@SuppressWarnings("nls")
	public static int getByteSizeToUse(long minBitSizeNecessary) {
		long tmp = minBitSizeNecessary >>> 3;
		if ((minBitSizeNecessary & 0x07) != 0) {
			tmp++;
		}
		tmp += FIXPOINT_RANGE_EXTENSION;
		if (tmp < Integer.MAX_VALUE) {
			return (int) tmp;
		}
		throw new ArithmeticException("overflow (to many bytes needed)");
	}

	/**
	 * method that estimates the bit size necessary to calculate the capacity of the given space.
	 * 
	 * @param space the space
	 * @return the estimated bit size to store the capacity
	 */
	public static long getEstimatedCapacityBitSize(Space space) {
		int[] lowerBoundaries = space.getLowerBoundaries();
		int[] upperBoundaries = space.getUpperBoundaries();

		long currentBitSize = 1;
		long lengthInDimension;
		for (int i = 0; i < lowerBoundaries.length; i++) {
			lengthInDimension = ((long) upperBoundaries[i]) - ((long) lowerBoundaries[i]) + 1L;
			currentBitSize += ceilLogarithm(lengthInDimension, floorLogarithm(lengthInDimension));
		}

		return currentBitSize;
	}
	
	/**
	 * method that estimates the bit size necessary to calculate the maximum distance of a data
	 * point and a corner of a bucket.
	 * 
	 * @param dimensions the number of dimensions
	 * @param points the number of points to summarize
	 * @return the estimated bit size to store the distances of 'points' points
	 */
	public static long getEstimatedCornerDistanceBitSize(int dimensions, int points) {
		// ld( ((2^31)^2) * n ) = 2 * ld(2^31) + ld(n) = 2 * 31 + ld(n) = 62 + ld(n)
		// max bit size of distance^2
		int maxDistanceBitSize = 62 + ceilLogarithm(dimensions, floorLogarithm(dimensions));
		// additional size needed to add 'points' such distances.
		// by sqrt 31 will be won back
		int possibleBitExtension = ceilLogarithm(points, floorLogarithm(points)) - 31;
		return maxDistanceBitSize + Math.max(0, possibleBitExtension);
	}
}
