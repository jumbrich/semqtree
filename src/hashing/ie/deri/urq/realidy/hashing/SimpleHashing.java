/**
 * 
 */
package ie.deri.urq.realidy.hashing;

import org.semanticweb.yars.nx.Node;

/**
 * @author hose
 *
 */
public class SimpleHashing extends QTreeHashing {
	
	private int alpha;
	@SuppressWarnings("unused")
	private int beta; 
	
	int minCharDec; 
	int maxCharDec; 

	
	public SimpleHashing(String hasherPrefix, int[] dimSpecMin, int[] dimSpecMax) {
	   super(hasherPrefix,dimSpecMin, dimSpecMax,true);
	   this.alpha = maxCharDec - minCharDec + 1; // space = decimal code 32, ~ = decimal code 126 --> 95 characters in the supported alphabet
		this.beta = 2 * alpha + 1; // fulfilling the following requirement: beta > 2 * alpha
		
		this.minCharDec = 32; // correspondung to white space
		this.maxCharDec = 126; // corresponding to ~
	}

	/**
	 * compute hash value for the given input string according to the following rule: H(s) = b1/beta + b2/beta^2 + b3/beta^3 + ...
	 * bi denotes the ascii code of the character at position i, beta > 2*alpha with alpha denoting the size of the alphabet
	 * 
	 * first characters of a string have the highest influence on the hash value
	 * @param input
	 * @return
	 */
	@SuppressWarnings("unused")
	private int getHashValue_FirstPosMostImportant(String input, int alpha, int beta,  int minCharDec, int maxCharDec, int dimSpecMin, int dimSpecMax){
		
		boolean debug = false;
		
		if (debug) System.out.println(input);
		
		double hashValue = 0.0;
		
		// run through all characters
		for (int i=0;i<input.length();i++){
			// get ASCII code value
			if (debug) System.out.print("char: "+input.charAt(i) + ", ");
			char ch = input.charAt(i);
			int asciiCode = (int) ch;
			if (debug) System.out.print("code: " + asciiCode+", ");
			
			if (asciiCode < minCharDec || asciiCode > maxCharDec){
				System.err.println("Character out of bounds -- unsupported character: " + ch);
			}
			
			// determine hashvalue
			//hashValue += asciiCode / ((double) Math.pow(beta, i+1));
			hashValue += (asciiCode - minCharDec + 1) / ((double) Math.pow(beta, i+1));
			//System.out.println(beta + " hoch " + (i+1) + " ist: "+ (((double) Math.pow(beta, i+1))));
			
			if (debug) System.out.println(hashValue);
		}
		if (debug) System.out.println("hash value: "+ hashValue);
		
		// scale 
		// maximum value for a string is 1
		double scaledDouble = hashValue * (dimSpecMax - dimSpecMin + 1) + dimSpecMin;
		if (debug) System.out.println("scaled "+scaledDouble);
		
		if (debug) System.out.println("rounded "+ ((int) scaledDouble));
		
		return (int) scaledDouble;
	}
	
	
	/**
	 * compute hash value for the given input string according to the following rule: H(s) = b1/beta + b2/beta^2 + b3/beta^3 + ...
	 * bi denotes the ascii code of the character at position i, beta > 2*alpha with alpha denoting the size of the alphabet
	 * 
	 * first characters of a string have the highest influence on the hash value
	 * 
	 * Thus, this method begins computing the hash value at the end of the given string, i.e., assigning inverse degrees of importance
	 * @param input
	 * @return
	 */
	@SuppressWarnings("unused")
	private int getHashValue_LastPosMostImportant(String input, int alpha, int beta, int minCharDec, int maxCharDec, int dimSpecMin, int dimSpecMax){
		
		boolean debug = false;
		
		if (debug) System.out.println(input);
		
		double hashValue = 0.0;
		
		// run through all characters
		for (int i=0;i<input.length();i++){
			// get ASCII code value
			if (debug) System.out.print("char: "+input.charAt(i) + ", ");
			char ch = input.charAt(i);
			int asciiCode = (int) ch;
			if (debug) System.out.print("code: " + asciiCode+", ");
			
			if (asciiCode < minCharDec || asciiCode > maxCharDec){
				System.err.println("Character out of bounds -- unsupported character: " + ch);
			}
			
			// determine hashvalue
			//hashValue += asciiCode / ((double) Math.pow(beta, input.length()-i));
			hashValue += (asciiCode - minCharDec + 1) / ((double) Math.pow(beta, input.length()-i));
			//System.out.println(beta + " hoch " + (i+1) + " ist: "+ (((double) Math.pow(beta, i+1))));
			
			if (debug) System.out.println(hashValue);
		}
		if (debug) System.out.println("hash value: "+ hashValue);
		
		// scale 
		// maximum value for a string is 1
		double scaledDouble = hashValue * (dimSpecMax - dimSpecMin + 1) + dimSpecMin;
		if (debug) System.out.println("scaled "+scaledDouble);
		
		if (debug) System.out.println("rounded "+ ((int) scaledDouble));
		
		return (int) scaledDouble;
	}
	
	
	/**
	 * 
	 * @param input
	 * @param minCharDec minimum byte code value of a character within the input string 
	 * @param maxCharDec maximum byte code value of a character within the input string
	 * @param dimSpecMin minimum value of the dimension in the QTree
	 * @param dimSpecMax maximum value of the dimension in the QTree
	 * @return
	 */
	@SuppressWarnings("unused")
	protected int getHashValue_AllPosEquallyImportant(String input, int minCharDec, int maxCharDec, int dimSpecMin, int dimSpecMax){
		
		boolean debug = false;
		
		// determine minimum and maximum possible hash value for an input string
		int maxValue = maxCharDec * input.length();
		if (debug) System.out.println("maxValue: "+maxValue);
		int minValue = minCharDec * input.length();
		if (debug) System.out.println("minValue: "+minValue);
		
		
		if (debug) System.out.println(input);
		
		double hashValue = 0.0;
		
		// run through all characters
		for (int i=0;i<input.length();i++){
			// get ASCII code value
			if (debug) System.out.print("char: "+input.charAt(i) + ", ");
			char ch = input.charAt(i);
			int asciiCode = (int) ch;
			if (debug) System.out.print("code: " + asciiCode+", ");
			
			if (asciiCode < minCharDec || asciiCode > maxCharDec){
				System.err.println("Character out of bounds -- unsupported character: " + ch + " "+asciiCode);
			}
			
			// determine hashvalue
			hashValue += asciiCode - minCharDec + 1;
			
			if (debug) System.out.println(hashValue);
		}
		if (debug) System.out.println("hash value: "+ hashValue);
		
		double scaledDouble = (hashValue - minValue) / (double) (maxValue - minValue) * (dimSpecMax - dimSpecMin) + dimSpecMin;
		if (debug) System.out.println("scaled "+scaledDouble+" int "+((int)scaledDouble));
		
		// just in case there is something wrong
		if (scaledDouble > dimSpecMax){
			System.err.println("cheated Hash: "+scaledDouble);
		}
		
		return (int) scaledDouble;
	}
	
		@Override
	public int objectHash(Node s) {
			return 	getHashValue_AllPosEquallyImportant(s.toString(), minCharDec, maxCharDec, getDimSpecMin()[0], getDimSpecMax()[0]);
	}

	@Override
	public int predicateHash(Node s) {
		return getHashValue_AllPosEquallyImportant(s.toString(), minCharDec, maxCharDec, getDimSpecMin()[1], getDimSpecMax()[1]);
	}

	@Override
	public int subjectHash(Node s) {
		return getHashValue_AllPosEquallyImportant(s.toString(), minCharDec, maxCharDec, getDimSpecMin()[2], getDimSpecMax()[2]);
	}
}