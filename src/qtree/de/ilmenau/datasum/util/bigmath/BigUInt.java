/*
 * 
 */
package de.ilmenau.datasum.util.bigmath;

import java.util.Arrays;

/**
 * This class represents a big unsigned integer value.<br>
 * <br>
 * For operations both {@link BigUInt}s must have the same internal byte size.
 * (because of optimizations) 
 * 
 * @author Christian Lemke
 * @version $Id$
 */
public final class BigUInt {

	/** the data as byte array */
	private byte[] data;


	/**
	 * constructor.
	 * 
	 * @param byteSize the number of bytes used to store the value (size of the internal byte array)
	 */
	public BigUInt(int byteSize) {
		this.data = new byte[byteSize];
	}

	/**
	 * constructor.
	 * 
	 * @param byteSize the number of bytes used to store the value (size of the internal byte array)
	 * @param initialValue the initial value (only the lower 63 bits are used -> so it is unsigned)
	 */
	public BigUInt(int byteSize, long initialValue) {
		this.data = new byte[Math.max(8, byteSize)];
		initialValue &= 0x7fffffffffffffffL;
		for (int i = 0; i < 8; i++) {
			this.data[i] = (byte) (initialValue & 0xff);
			initialValue >>>= 8;
		}
	}

	/**
	 * constructor.
	 * 
	 * @param byteSize the number of bytes used to store the value (size of the internal byte array)
	 * @param initialValue the initial value (only the lower 63 bits are used -> so it is unsigned)
	 */
	public BigUInt(int byteSize, String initialValue) {
		long inputLength = ((initialValue.length() * 10000000L) / 3010299L) + 1L; // / lg(2) with 7 fix points
		int byteLength = (int) (inputLength >>> 3);
		if ((inputLength & 0x07) != 0) {
			byteLength++;
		}
		byteSize = Math.max(byteSize, byteLength);
		this.data = new byte[byteSize];

		int digit;
		for (int i = 0; i < initialValue.length(); i++) {
			//digit = Integer.parseInt(Character.toString(initialValue.charAt(i)));
			digit = initialValue.charAt(i) - '0';
			this.partialMul(10);
			this.add(digit);
		}
	}

	/**
	 * method that adds the given {@link BigUInt} to the current {@link BigUInt}.
	 * 
	 * @param incrementor the {@link BigUInt} to add
	 * @throws DifferentByteSizeException if the operators differ in the internal byte size
	 */
	public void add(BigUInt incrementor) throws DifferentByteSizeException {
		if (this.data.length != incrementor.data.length) {
			throw new DifferentByteSizeException();
		}

		int tmp = 0;
		for (int i = 0; i < this.data.length; i++) {
			tmp = (this.data[i] & 0xff) + (incrementor.data[i] & 0xff) + ((tmp >>> 8) & 0xff);
			this.data[i] = (byte) (tmp & 0xff);
		}
	}

	/**
	 * method that adds the given int to the current {@link BigUInt}.
	 * (only the lower 31 bits are used)
	 * 
	 * @param incrementor the int to add
	 */
	public void add(int incrementor) {
		incrementor &= 0x7fffffff;
		for (int i = 0; i < this.data.length; i++) {
			incrementor += (this.data[i] & 0xff);
			this.data[i] = (byte) (incrementor & 0xff);
			incrementor >>>= 8;
		}
	}

	/**
	 * method that adds the given long to the current {@link BigUInt}.
	 * (only the lower 63 bits are used)
	 * 
	 * @param incrementor the long to add
	 */
	public void add(long incrementor) {
		incrementor &= 0x7fffffffffffffffL;
		for (int i = 0; i < this.data.length; i++) {
			incrementor += (this.data[i] & 0xff);
			this.data[i] = (byte) (incrementor & 0xff);
			incrementor >>>= 8;
		}
	}

	/**
	 * method that calculates the logarithm to the basis 2 (rounded up) of the current {@link BigUInt}.
	 * 
	 * @param floorLogarithm the logarithm to the basis 2 (rounded down)
	 * @return the ceil logarithm
	 */
	public long ceilLogarithm(long floorLogarithm) {
		long firstBitSetPos = 0;
		byte workingByte;

		outerLoop: for (int i = 0; i < this.data.length; i++) {
			workingByte = this.data[i];
			for (byte j = 0; j < 8; j++) {
				if ((workingByte & 0x01) == 0) {
					workingByte >>>= 1;
				} else {
					firstBitSetPos = (i << 3) + j;
					break outerLoop;
				}
			}
		}
		if (floorLogarithm > firstBitSetPos) {
			return floorLogarithm + 1;
		}
		return floorLogarithm;
	}

	/**
	 * method that divides the current {@link BigUInt} by the given {@link BigUInt}.
	 * it overwrites the value of divisorAndRemainder with the remainder.
	 * 
	 * @param divisorAndRemainder the divisor (and after calculation the remainder)
	 * @throws DifferentByteSizeException if the operators differ in the internal byte size
	 */
	@SuppressWarnings("nls")
	public void div(BigUInt divisorAndRemainder) throws DifferentByteSizeException {
		if (this.data.length != divisorAndRemainder.data.length) {
			throw new DifferentByteSizeException();
		}

		long divisorLen = divisorAndRemainder.floorLogarithm();
		if (divisorLen == 0) { // divisor is 1 or 0 !
			if ((divisorAndRemainder.data[0] & 0x01) == 0) {
				throw new ArithmeticException("/ by zero");
			}
			// else the remainder is 0 -> division by one
			divisorAndRemainder.setZero();
			return;
		}

		long dividendLen = this.floorLogarithm();
		if (dividendLen < divisorLen) {
			divisorAndRemainder.getValueFrom(this);
			this.setZero();
			return;
		}

		if (dividendLen == divisorLen) {
			if (this.isLess(divisorAndRemainder)) {
				divisorAndRemainder.getValueFrom(this);
				this.setZero();
				return;
			} else if (this.isEqual(divisorAndRemainder)) {
				// both numbers are equal -> result = 1; remainder = 0
				this.setOne();
				divisorAndRemainder.setZero();
				return;
			}
		}

		// end of exception handling

		long totalLen = dividendLen - divisorLen + 1;
		long internalByteSize = (totalLen >>> 3) + 1 + this.data.length;
		internalByteSize = Math.min(Integer.MAX_VALUE, internalByteSize);
		BigUInt tmpRemainder = new BigUInt((int) internalByteSize);
		BigUInt tmpDividend = new BigUInt((int) internalByteSize);
		System.arraycopy(divisorAndRemainder.data, 0, tmpRemainder.data, 0, this.data.length);
		System.arraycopy(this.data, 0, tmpDividend.data, 0, this.data.length);

		this.setZero();
		tmpRemainder.shiftLeft(totalLen);

		for (long i = totalLen; i >= 0; i--) {
			if (!tmpDividend.isLess(tmpRemainder)) {
				this.data[(int) (i >>> 3)] += 1 << (i & 0x07);
				tmpDividend.sub(tmpRemainder);
			}
			tmpRemainder.shiftRight(1);
		}
		System.arraycopy(tmpDividend.data, 0, divisorAndRemainder.data, 0, this.data.length);
	}

	/**
	 * method that calculates the logarithm to the basis 2 (rounded up) of the current {@link BigUInt}.
	 * 
	 * @return the floor logarithm
	 */
	public long floorLogarithm() {
		byte workingByte;

		for (int i = this.data.length - 1; i >= 0; i--) {
			workingByte = this.data[i];
			for (byte j = 7; j >= 0; j--) {
				if ((workingByte & 0x80) == 0) {
					workingByte <<= 1;
				} else {
					return (i << 3) + j;
				}
			}
		}
		return 0; // normally the ld(0) is not definied
	}

	/**
	 * method returns the internal byte size.
	 * 
	 * @return the size of the internal byte array
	 */
	public int getByteSize() {
		return this.data.length;
	}

	/**
	 * method that returns only the lower 63 bits of the current {@link BigUInt} so it is always
	 * positive.
	 * 
	 * @return the lower 63 bits as long
	 */
	public long getLower63Bits() {
		long tmp;

		if (this.data.length < 8) {
			tmp = 0;
			for (int i = this.data.length - 1; i >= 0; i--) {
				tmp = (tmp << 8) | (this.data[i] & 0xff);
			}
		} else {
			tmp = this.data[7] & 0x7f;
			for (int i = 6; i >= 0; i--) {
				tmp = (tmp << 8) | (this.data[i] & 0xff);
			}
		}
		return tmp;
	}

	/**
	 * method that copies the value (byte array) from the given {@link BigUInt} to the current
	 * {@link BigUInt}.
	 * 
	 * @param toCopyFrom the {@link BigUInt} to "copy"
	 * @throws DifferentByteSizeException if the operators differ in the internal byte size
	 */
	public void getValueFrom(BigUInt toCopyFrom) throws DifferentByteSizeException {
		if (this.data.length != toCopyFrom.data.length) {
			throw new DifferentByteSizeException();
		}

		System.arraycopy(toCopyFrom.data, 0, this.data, 0, this.data.length);
	}

	/**
	 * method that returns whether the current {@link BigUInt} is equal to the given {@link BigUInt}.
	 * 
	 * @param comparator the {@link BigUInt} to compare with
	 * @return <tt>true</tt> if both {@link BigUInt}s are equal
	 * @throws DifferentByteSizeException if the operators differ in the internal byte size
	 */
	public boolean isEqual(BigUInt comparator) throws DifferentByteSizeException {
		if (this.data.length != comparator.data.length) {
			throw new DifferentByteSizeException();
		}

		for (int i = 0; i < this.data.length; i++) {
			if (this.data[i] != comparator.data[i]) {
				return false;
			}
		}
		return true;
	}

	/**
	 * method that returns whether the current {@link BigUInt} is greater than the given {@link BigUInt}.
	 * 
	 * @param comparator the {@link BigUInt} to compare with
	 * @return <tt>true</tt> if the current {@link BigUInt} is greater than the given {@link BigUInt}
	 * @throws DifferentByteSizeException if the operators differ in the internal byte size
	 */
	public boolean isGreater(BigUInt comparator) throws DifferentByteSizeException {
		if (this.data.length != comparator.data.length) {
			throw new DifferentByteSizeException();
		}

		for (int i = this.data.length - 1; i >= 0; i--) {
			if ((this.data[i] & 0xff) > (comparator.data[i] & 0xff)) {
				return true;
			} else if ((this.data[i] & 0xff) != (comparator.data[i] & 0xff)) {
				return false;
			}
		}
		return false;
	}

	/**
	 * method that returns whether the current {@link BigUInt} is less than the given {@link BigUInt}.
	 * 
	 * @param comparator the {@link BigUInt} to compare with
	 * @return <tt>true</tt> if the current {@link BigUInt} is less than the given {@link BigUInt}
	 * @throws DifferentByteSizeException if the operators differ in the internal byte size
	 */
	public boolean isLess(BigUInt comparator) throws DifferentByteSizeException {
		if (this.data.length != comparator.data.length) {
			throw new DifferentByteSizeException();
		}

		for (int i = this.data.length - 1; i >= 0; i--) {
			if ((this.data[i] & 0xff) < (comparator.data[i] & 0xff)) {
				return true;
			} else if ((this.data[i] & 0xff) != (comparator.data[i] & 0xff)) {
				return false;
			}
		}
		return false;
	}

	/**
	 * method that calculates a partial multiplication with the given factor (<tt>long</tt>).
	 * (only the lower 63 bits are used -> so it is unsigned)
	 * 
	 * @param factor the factor
	 */
	public void partialMul(long factor) {
		factor &= 0x7fffffffffffffffL;
		long tmp = 0;
		for (int i = 0; i < this.data.length; i++) {
			tmp = (this.data[i] & 0x000000ff) * (factor & 0xffffffffL)
					+ ((tmp >>> 8) & 0xffffffffL);
			this.data[i] = (byte) (tmp & 0x000000ff);
		}
	}

	/**
	 * method that sets the current {@link BigUInt} to 1.
	 */
	public void setOne() {
		this.setZero();
		this.data[0] = 1;
	}

	/**
	 * method that sets the current {@link BigUInt} to 0.
	 */
	public void setZero() {
		Arrays.fill(this.data, (byte) 0);
	}

	/**
	 * method that does a shift left.
	 * 
	 * @param number the amount to shift
	 */
	public void shiftLeft(long number) {
		int byteShift = (int) (number >>> 3);
		int bitShift = (int) (number & 0x07);

		int tmp;
		for (int i = this.data.length - 1; i >= byteShift; i--) {
			if (i > byteShift) {
				tmp = ((this.data[i - byteShift - 1] & 0xff) >>> (8 - bitShift)) & 0xff;
			} else {
				tmp = 0;
			}
			this.data[i] = (byte) ((((this.data[i - byteShift] & 0xff) << bitShift) | tmp) & 0xff);
		}

		for (int i = 0; i < byteShift; i++) {
			this.data[i] = 0;
		}
	}

	/**
	 * method that does a shift right.
	 * 
	 * @param number the amount to shift
	 */
	public void shiftRight(long number) {
		int byteShift = (int) (number >>> 3);
		int bitShift = (int) (number & 0x07);

		int tmp;
		for (int i = byteShift; i < this.data.length; i++) {
			if (i + 1 < this.data.length) {
				tmp = (this.data[i + 1] << (8 - bitShift)) & 0xff;
			} else {
				tmp = 0;
			}
			this.data[i - byteShift] = (byte) ((((this.data[i] & 0xff) >>> bitShift) | tmp) & 0xff);
		}

		tmp = Math.max(0, this.data.length - byteShift);
		for (int i = this.data.length - 1; i >= tmp; i--) {
			this.data[i] = 0;
		}
	}

	/**
	 * method that calculates the sqare root over an approximation.
	 *   sqrt(a) -> x_neu = 1/2 * ((a / x_alt) + x_alt) 
	 * 
	 * @param maxIterations maximum iterations
	 */
	public void sqrt(long maxIterations) {
		maxIterations &= 0x7fffffffffffffffL;
		BigUInt a = new BigUInt(this.data.length);
		BigUInt x_alt = new BigUInt(this.data.length);
		// this = x_neu
		BigUInt tmp = new BigUInt(this.data.length);
		try {
			a.getValueFrom(this);
			this.setOne();
			for (long i = 0; i < maxIterations; i++) {
				if (x_alt.isEqual(this)) {
					// debug output: System.out.println("--> " + i);
					break;
				}
				x_alt.getValueFrom(this);
				this.getValueFrom(a);
				tmp.getValueFrom(x_alt);
				this.div(tmp); // a / x_alt
				this.add(x_alt); // + x_alt
				this.shiftRight(1);
			}
		} catch (DifferentByteSizeException e) {
			// could not happen
		}
	}

	/**
	 * method that subtracts the given {@link BigUInt} from the current {@link BigUInt}.
	 * 
	 * @param decrementor the {@link BigUInt} to add subtract
	 * @throws DifferentByteSizeException if the operators differ in the internal byte size
	 */
	public void sub(BigUInt decrementor) throws DifferentByteSizeException {
		if (this.data.length != decrementor.data.length) {
			throw new DifferentByteSizeException();
		}

		int tmp = 0;
		for (int i = 0; i < this.data.length; i++) {
			tmp = (this.data[i] & 0xff) - (decrementor.data[i] & 0xff) - ((-(tmp >>> 8)) & 0xff);
			this.data[i] = (byte) (tmp & 0xff);
		}
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		char[] digitToChar = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' };
		BigUInt base = new BigUInt(this.data.length);
		BigUInt backup = new BigUInt(this.data.length);
		long floorLogarithm = this.floorLogarithm();
		long ceilLogarithm = this.ceilLogarithm(floorLogarithm);

		long outputLength = ((ceilLogarithm * 3010299L) / 10000000L) + 1L; // lg(2) with 7 fix points
		char[] result = new char[(int) outputLength];

		try {
			backup.getValueFrom(this);
		} catch (DifferentByteSizeException e) {
			// could not happen
		}
		for (int i = (int) (outputLength - 1); i >= 0; i--) {
			base.data[0] = 10;
			try {
				backup.div(base);
			} catch (Exception e) {
				// could not happen
			}
			result[i] = digitToChar[base.data[0]];
		}
		return new String(result);
	}
}
