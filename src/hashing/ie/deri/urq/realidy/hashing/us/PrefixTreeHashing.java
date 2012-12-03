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

import ie.deri.urq.realidy.hashing.HashingFactory;
import ie.deri.urq.realidy.hashing.QTreeHashing;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;



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
public class PrefixTreeHashing extends QTreeHashing{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public static int ATTR_HASH_CONC_LEN = 2;
	public static boolean RESPECT_LENGTH_FOR_INT_HASHING = true;

	private final PrefixTree mHash;
	private QTreeHashing _stringHasher;

	/**
	 * Builds the initial trie.
	 * 
	 * Uses PrefixTree.ini in order to read it, if exists. Otherwise uses PrefixTree.dat in order to construct it
	 * and to write PrefixTree.ini.
	 * @param hasherPrefix 
	 * @param dimSpecMax 
	 * @param dimSpecMin 
	 */
	public PrefixTreeHashing(String hasherPrefix, final int[] dimSpecMin, final int[] dimSpecMax) {
		super(hasherPrefix,dimSpecMin,dimSpecMax, false);
		mHash = new PrefixTree();
		mHash.init();
		
		_stringHasher = HashingFactory.createHasher(HashingFactory.HASHER_ADVANCEDNAMESPACE, dimSpecMin, dimSpecMax);
	}


	/**
	 * Prints the trie.
	 */
	public void printTree() {
		mHash.printTree();
	}

	public int hash(String s){
		String binHashValue = mHash.findKey(s.toString()).trim();
		if(binHashValue.length() == 0) binHashValue="0";
		long hash = Long.parseLong(binHashValue, 2);
		
		int hash1 = scaleRange((int)hash, 0, 10000000, getDimSpecMin()[0], getDimSpecMax()[0]);
//		System.out.println("hash: "+hash1+" of "+hash);
		return hash1;
	}

	String reverseString(final String s) {
		String r = "";
		for (int i=0;i<s.length();i++) r += s.charAt(s.length()-i-1);
		return r;
	}

	public int objectHash(Node s) {
		return hash(s.toN3());
//		
//		Integer hash = null;
//		if( s instanceof Resource) {
//			hash = hash(s.toString());
//		}
//		if(hash == null || hash==-1) hash = _stringHasher.objectHash(s);
//		
//		else return hash;
//		
//		return scaleRange(hash, 0,Integer.MAX_VALUE, getDimSpecMin()[0], getDimSpecMax()[0]);
		
	}

	public int predicateHash(Node s) {
		Integer hash = null;
		if( s instanceof Resource) {
			hash = hash(s.toString());
		}
		if(hash==null || hash==-1) hash = _stringHasher.predicateHash(s);
		else return hash;
		return scaleRange(hash, 0,Integer.MAX_VALUE, getDimSpecMin()[0], getDimSpecMax()[0]);
	}

	public int subjectHash(Node s) {
		return hash(s.toN3());
	}
}