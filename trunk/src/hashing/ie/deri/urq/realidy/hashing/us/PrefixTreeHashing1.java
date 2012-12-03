/**
 * Copyright (c) 2007 The UniStore Team,
 *                    All Rights Reserved.
 *
 * This file is part of the UniStore package.
 * UniStore homepage: http://www.tu-ilmenau.de/fakia/The-b-UniStore-b.6485.0.html 
 *
 * The UniStore package is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License (GPL) as
 * published by the Free Software Foundation; either version 3 of
 * the License, or (at your option) any later version.
 *
 * This package is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; see the file LICENSE.
 * If not you can find the GPL at http://www.gnu.org/copyleft/gpl.html
 */

package ie.deri.urq.realidy.hashing.us;

import org.semanticweb.yars.nx.Node;

/*
 * Created on 15.05.2006
 */
// package pgrid.similarity.util.hashing;
// 
// import pgrid.interfaces.basic.PGridP2P;
// import pgrid.similarity.Const;

/**
 * Prefix trie hashing based on PrefixTree.
 * 
 * @author karn
 */
public class PrefixTreeHashing1 extends SimpleHashing1 implements Hashing {

	public static int ATTR_HASH_CONC_LEN = 2;
	public static boolean RESPECT_LENGTH_FOR_INT_HASHING = true;

	private PrefixTree mHash;
	
	/**
	 * Builds the initial trie.
	 * 
	 * Uses PrefixTree.ini in order to read it, if exists. Otherwise uses PrefixTree.dat in order to construct it
	 * and to write PrefixTree.ini.
	 */
	public PrefixTreeHashing1(String hasherPrefix, final int[] dimSpecMin, final int[] dimSpecMax) {
		super(hasherPrefix,  dimSpecMin,dimSpecMax);
		mHash = new PrefixTree();
		mHash.init();
	}
	
	/**
	 * @deprecated
	 */
	public String hashKey(Object o, int col) {
		return mHash.findKey(Integer.toString(col))+mHash.findKey(o.toString());
	}

	/**
	 * Computes a hashkey of concatenation col#o by concatenating hashKey(col)#hashKey(o).
	 * 
	 * If o is a String, h(o) is computed from the trie, otherwise by the super implementation.
	 * h(col) is always read from the trie.
	 * 
	 * @param o the value to hash
	 * @param col the column name
	 * @return h(col)#h(o)
	 */
	public String hashKey(Object o, String col) {
		String attrHash = hashKey(col);
		if (-1 < ATTR_HASH_CONC_LEN && attrHash.length() > ATTR_HASH_CONC_LEN) {
			attrHash = attrHash.substring(0,ATTR_HASH_CONC_LEN);
		}
		return attrHash+hashKey(o);
	}
	
	/**
	 * @see Hashing
	 */
	public String hashKey(int oid) {
//		return hashKey(new Integer(oid));
		return hashKey(oid,RESPECT_LENGTH_FOR_INT_HASHING);
	}
	
	/**
	 * Computes a hashkey of value o.
	 * 
	 * If o is a String, h(o) is computed from the trie, otherwise by the super implementation.
	 *
	 * @param o the value to hash
	 * @param col the column name
	 * @return h(o)
	 */
	public String hashKey(Object o) {
//		System.err.println("called PrefixTreeHashing.hashKey(Object)... object of type: "+o.getClass().getName());
		// if it's a String we look in the trie
		if (o instanceof String) {
//			o = ((String)o).replace("<http://", "");
			return mHash.findKey(o.toString());
		}
//		if (o instanceof String) return mHash.findKey(reverseString(o.toString()));
		// else we delegate to our super implementation
		return super.hashKey(o);
	}
	
	/**
	 * Prints the trie.
	 */
	public void printTree() {
		mHash.printTree();
	}
	
	/**
	 * Determines the maximum possible key whith prefix hashKey(colId). The returned value corresponds
	 * to the decimal value of the binary key k=hashKey(colId)+suffix, where suffix consists of only '1' and
	 * k has the same length as the local path.
	 */
	public int getMaxKey(String colId) {
		String max = hashKey(colId);
		String localPath = PATH;
//		String localPath = "00100";
//		while (max.length() < MAXBITS) max += '1';
		while (max.length() < mHash.getDepth()) max += '1';
		System.err.println("max: "+max+", length: "+max.length()+", colId: "+colId);
//		System.exit(1);
		return Integer.parseInt(max,2);
	}

	public long getMaxKeyLong(String colId) {
		String max = hashKey(colId);
		String localPath = PATH;
//		String localPath = "00100";
//		while (max.length() < MAXBITS) max += '1';
		while (max.length() < mHash.getDepth()) max += '1';
		return Long.parseLong(max,2);
	}

	/**
	 * compute hash coordinates
	 * 
	 * @param item
	 * @return
	 */
	public int[] getHashCoordinates(String[] item, int[] dimSpecMin, int[] dimSpecMax){
		int[] hashCoordinates = new int[item.length];
		
		for (int i=0;i<item.length;i++){
//			String binHashValue = hashKey(reverseString(item[i]));
			String binHashValue = hashKey(item[i]);

			if (binHashValue.equals("")) binHashValue = "0";
//			System.err.println("length of binary key: "+binHashValue.length());
			
			int hashValue = getIntValue(binHashValue);
//			double scaledDouble = (double)hashValue / (double)getMaxKey("") * (dimSpecMax[i] - dimSpecMin[i]) + dimSpecMin[i];
			double scaledDouble = (double)hashValue / (double)Integer.MAX_VALUE * (dimSpecMax[i] - dimSpecMin[i]) + dimSpecMin[i];

//			long hashValue = getLongValue(binHashValue);
//			double scaledDouble = (double)hashValue / (double)getMaxKeyLong("") * (dimSpecMax[i] - dimSpecMin[i]) + dimSpecMin[i];
			
//			double scaledDouble = (hashValue - minValue) / (double) (maxValue - minValue) * (dimSpecMax - dimSpecMin) + dimSpecMin;

//			if (scaledDouble > 1000) {System.err.println(binHashValue+"="+hashValue+"; max="+getMaxKey(""));System.exit(1);}
			if (scaledDouble > dimSpecMax[i]) {System.err.println("determined hash hvalue is above the upper limit: "+scaledDouble+">"+dimSpecMax[i]);System.exit(1);}
//			System.err.println("\tscaling: "+(double)hashValue+"/"+(double)getMaxKey("")+"*("+dimSpecMax[i]+"-"+dimSpecMin[i]+")+"+dimSpecMin[i]);

//			/*if (binHashValue.equals("0"))*/ System.err.println("val: "+item[i]+", key unscaled: "+hashValue+", key scaled: "+scaledDouble);
			hashCoordinates[i] = (int)scaledDouble;
		}
		
		return hashCoordinates;
	}
	
	
	public int generalHash(String s ){
		String binHashValue = hashKey(s);

		if (binHashValue.equals("")) binHashValue = "0";
//		System.err.println("length of binary key: "+binHashValue.length());
		
		int hashValue = getIntValue(binHashValue);
//		double scaledDouble = (double)hashValue / (double)getMaxKey("") * (dimSpecMax[i] - dimSpecMin[i]) + dimSpecMin[i];
		double scaledDouble = (double)hashValue / (double)Integer.MAX_VALUE * (getDimSpecMax()[0] - getDimSpecMin()[0]) + getDimSpecMin()[0];

//		long hashValue = getLongValue(binHashValue);
//		double scaledDouble = (double)hashValue / (double)getMaxKeyLong("") * (dimSpecMax[i] - dimSpecMin[i]) + dimSpecMin[i];
		
//		double scaledDouble = (hashValue - minValue) / (double) (maxValue - minValue) * (dimSpecMax - dimSpecMin) + dimSpecMin;

//		if (scaledDouble > 1000) {System.err.println(binHashValue+"="+hashValue+"; max="+getMaxKey(""));System.exit(1);}
		if (scaledDouble > getDimSpecMax()[0]) {System.err.println("determined hash hvalue is above the upper limit: "+scaledDouble+">"+getDimSpecMax()[0]);System.exit(1);}
//		System.err.println("\tscaling: "+(double)hashValue+"/"+(double)getMaxKey("")+"*("+dimSpecMax[i]+"-"+dimSpecMin[i]+")+"+dimSpecMin[i]);

//		/*if (binHashValue.equals("0"))*/ System.err.println("val: "+item[i]+", key unscaled: "+hashValue+", key scaled: "+scaledDouble);
		return (int)scaledDouble;
	}

	String reverseString(String s) {
		String r = "";
		for (int i=0;i<s.length();i++) r += s.charAt(s.length()-i-1);
		return r;
	}

	@Override
	public int objectHash(Node s) {
		return generalHash(s.toN3());
	}

	@Override
	public int predicateHash(Node s) {
		return generalHash(s.toN3());
	}

	@Override
	public int subjectHash(Node s) {
		return generalHash(s.toN3());
	}
}
