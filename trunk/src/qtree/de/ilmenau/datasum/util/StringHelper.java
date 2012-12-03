/*
 *
 */
package de.ilmenau.datasum.util;

import java.util.Arrays;
import java.util.Vector;

/**
 * This class contains some helper methods for String handling.
 * 
 * @author Christian Lemke
 * @version $Id$
 */
@SuppressWarnings("nls")
public abstract class StringHelper {
	
	/**
	 * computes the longest common prefixes and suffixes and removes them from the tuples
	 * @param stringData
	 */
	public static void trimmingCommonPrefixesAndSuffixes(Vector<String[]> stringData){
		
		boolean debug = false;
		
		// determine longest common prefixes 
		
		// initialize with the first element
		String[] prefixes = stringData.firstElement().clone(); 
		
		// for each tuple, i.e., item in stringData
		for (int currPos = 1; currPos<stringData.size();currPos++){
			//if (debug) System.out.println("Prefixes: "+Arrays.toString(prefixes));

			String[] currItem = stringData.elementAt(currPos); 
			//if (debug) System.out.println("Item "+Arrays.toString(currItem));
			
			// for each dimension
			for (int i=0;i < currItem.length;i++){
				
				String prefix = Str.longestCommonPrefix(prefixes[i], currItem[i]);
				//if (debug) System.out.println("prefix: " +  prefix);
				// if there is no common prefix, prefix has the value ""
				prefixes[i] = prefix;
			}
		}
		
		
		// determine longest common suffix
		
		// initialize with the first element
		String[] suffixes = stringData.firstElement().clone(); 
		
		// for each tuple, i.e., item in stringData
		for (int currPos = 1; currPos<stringData.size();currPos++){
			//if (debug) System.out.println("Suffixes: "+Arrays.toString(suffixes));

			String[] currItem = stringData.elementAt(currPos); 
			//if (debug) System.out.println("Item "+Arrays.toString(currItem));
			
			// for each dimension
			for (int i=0;i < currItem.length;i++){
				
				String suffix = Str.longestCommonSuffix(suffixes[i], currItem[i]);
				//if (debug) System.out.println("suffix: " +  suffix);
				// if there is no common suffix, suffix has the value ""
				suffixes[i] = suffix;
			}
		}
		
		
		
		if (debug) System.out.println("Prefixes: "+Arrays.toString(prefixes));
		if (debug) System.out.println("Suffixes: "+Arrays.toString(suffixes));
		// remove common prefix
		// for all input tuples
		for (String[] currItem: stringData){
			
			//System.out.println("Item before: "+Arrays.toString(currItem));
			// for all dimensions
			for (int i=0;i < prefixes.length;i++){
				currItem[i] = currItem[i].replace(prefixes[i], "");
				currItem[i] = currItem[i].replace(suffixes[i], "");
			}
			//System.out.println("Item after: "+Arrays.toString(currItem));
		}
	}
	

	/**
	 * method that creates a string that represents the array.
	 * 
	 * @param inputArray the int array
	 * @return string representation
	 */
	public static String arrayToString(int[] inputArray) {
		String result = "[";

		for (int i = 0; i < inputArray.length; i++) {
			Object currObject = inputArray[i];
			result += currObject.toString();
			if (i < inputArray.length - 1) {
				result += ",";
			}
		}
		result += "]";

		return result;
	}

	/**
	 * method that creates a string that represents the array.
	 * 
	 * @param inputArray the Object array
	 * @return string representation
	 */
	public static String arrayToString(Object[] inputArray) {
		String result = "[";

		for (int i = 0; i < inputArray.length; i++) {
			Object currObject = inputArray[i];
			result += currObject.toString();
			if (i < inputArray.length - 1) {
				result += ",";
			}
		}
		result += "]";

		return result;
	}
}
