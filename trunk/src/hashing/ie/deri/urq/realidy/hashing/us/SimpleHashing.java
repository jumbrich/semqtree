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

import ie.deri.urq.realidy.hashing.QTreeHashing;

import org.semanticweb.yars.nx.Node;



/*
 * Created on 19.05.2006
 */
// package pgrid.similarity.util.hashing;
// 
// import pgrid.interfaces.basic.PGridP2P;
// import pgrid.similarity.Const;
// import unistore.api.*;
// import unistore.api.util.*;

/**
 * Implements a basic hashing facility that can be extended by sophisticated methods.
 * 
 * @author karn
 */
public abstract class SimpleHashing extends QTreeHashing implements Hashing  {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    static int MAXBITS=31;
    static String PATH;
    static {for (int i=0;i<MAXBITS;i++) PATH += "0";}
    static int COLUMN_BITS=1;

    public SimpleHashing(String hasherPrefix, int[] dimSpecMin, int[] dimSpecMax) {
    	super(hasherPrefix,dimSpecMin,dimSpecMax,true);
    }

    /**
     * @param intVal
     * @param respect_length
     * @return
     */
    public static String hashKey(int intVal, boolean respect_length) {
	String result = Integer.toBinaryString(intVal);
	/*		UniStoreProperties props;
		props = UniStoreImpl.sharedInstance().getUniStoreProperties();*/
	/*		if (null == props) {
			props = new UniStoreProperties();
			props.loadDefaultProperties();
		}*/
	if (respect_length)
	{
	    while (result.length() < MAXBITS) result = '0' + result;
	    // the resulting binary string could be longer than we allow...
	    if (result.length() > MAXBITS) {
		int oldLength = result.length();
		// so we set it to the maximum '11...1' if > 0, to the minimum '00...0' if < 0
		char bit = (0 < intVal)? '1' : '0';
		result = "";
		while (result.length() < MAXBITS) result = bit + result;
		System.err.println("SimpleHashing.hashKey(int, boolean): Warning: Computed too long binary string (len(hash("+intVal+"))="+oldLength+"), setting to '"+result+"'!");
	    }
	}
	return result;
    }

    /**
     * @deprecated
     */
    public abstract String hashKey(Object o, int col);

    /**
     * @see Hashing
     */
    public abstract String hashKey(Object o, String col);

    /**
     * @see Hashing
     */
    public String hashKey(int oid) {
	return hashKey(oid,true);
    }

    /**
     * Computes a hashkey of value o
     * 
     * If o is an Integer, the binary String of the value is returned. Otherwise, the binary
     * String of o.hashCode() is returned.
     * 
     * @param o the value to hash
     * @param col the column name
     * @return h(o)
     */
    public String hashKey(Object o) {
	if (o instanceof Integer) return hashKey(((Integer)o).intValue(),true);
	return Integer.toBinaryString(o.hashCode());
    }

    /**
     * Determines the previous possible key for the local peer, no prefix is preserved.
     * This means, the last '1' is inverted and every bit following is set to '1'.
     * The returned String is of same length as the local path. Returns null if no such key exists.
     */
    public String getPrevPath() {
	return getPrevPathForPrefix(0);
    }

    /**
     * Determines the previous possible key for the local peer while preserving prefix of length prefixLen.
     * This means, the last '1' is inverted, if it is not part of the prefix, and every bit following is set to '1'.
     * The returned String is of same length as the local path.
     * Returns null if no such key exists.
     * 
     * @param prefixLen number of bits from the beginning that shall be preserved
     * @return the previous path preceding the local peer's path while preserving a prefix of length prefixLen
     */
    protected String getPrevPathForPrefix(int prefixLen) {
	return getPathForPrefix(prefixLen,false);
    }

    /**
     * Determines the next possible key for the local peer, no prefix is preserved.
     * This means, the last '0' is inverted and every bit following is set to '0'.
     * The returned String is of same length as the local path. Returns null if no such key exists.
     */
    public String getNextPath() {
	return getNextPathForPrefix(0);
    }

    /**
     * Determines the next possible key for the local peer while preserving prefix of length prefixLen.
     * This means, the last '0' is inverted, if it is not part of the prefix, and every bit following is set to '0'.
     * The returned String is of same length as the local path.
     * Returns null if no such key exists.
     * 
     * @param prefixLen number of bits from the beginning that shall be preserved
     * @return the next path following the local peer's path while preserving a prefix of length prefixLen
     */
    protected String getNextPathForPrefix(int prefixLen) {
	return getPathForPrefix(prefixLen,true);
    }

    /**
     * Determines the next or previous possible key for the local peer while preserving prefix of length prefixLen.
     * 
     * @param prefixLen number of bits from the beginning that shall be preserved
     * @param direction false: previous path, true: next path
     * @return the next/previous path following/preceding the local peer's path while preserving a prefix of length prefixLen
     */
    private String getPathForPrefix(int prefixLen, boolean next) {
	// 		String path = PGridP2P.sharedInstance().getLocalPath();
	String path = "000";
	// find last position pos that is 0 or 1, resp.
	char bitToFind = next? '0' : '1';
	int pos = path.lastIndexOf(bitToFind);
	//		System.err.println("prefix len: "+prefixLen+", pos: "+pos);
	StringBuffer modPath = new StringBuffer(path);
	if (prefixLen-1 < pos) {
	    // change the found 0 to a 1
	    char newBit = next? '1' : '0';
	    modPath.setCharAt(pos,newBit);
	    //set every position i>pos to 0 or 1, resp.
	    newBit = next? '0' : '1';
	    for (int i = pos+1; i < modPath.length(); ++i) modPath.setCharAt(i,newBit);
	    return modPath.toString();
	}
	// no next path
	return null;
    }

    /**
     * @deprecated
     * 
     * Determines the next possible key for the local peer with prefix hashKey(colId).
     * Returns null if no such key exists.
     */
    public String getNextPath(Object colId) {
	// 		switch (Const.CURR_PROCESSING_STRAT) {
	// 			case COLUMN_AS_STRING: {
	return getNextPath((String)colId);
	/*			}
			case COLUMN_AS_INT: {
				return getNextPath(((Integer)colId).intValue());
			}
			default: {
				System.err.println("Encountered unknown processing strategy: "+Const.CURR_PROCESSING_STRAT+" ...exiting!");
				System.exit(1);
			}
		}*/
	// 		return null;
    }

    /**
     * Determines the next possible key for the local peer with prefix hashKey(colId).
     * Returns null if no such key exists.
     */
    public String getNextPath(String colId) {
	return getNextPathForPrefix(hashKey(colId).length());
    }

    /**
     * @deprecated
     * 
     * Determines the next possible key for the local peer with prefix hashKey(colId).
     * Returns null if no such key exists. The returned String is of length Const.MAXBITS.
     */
    public String getNextPath(int colId) {
	return getNextPath( (((Integer)colId) + 1 << MAXBITS - COLUMN_BITS) - 1, PATH);
    }

    /**
     * @deprecated
     * 
     * Determines the path preceding the local peer's path while preserving prefix hashKey(colId).
     * Returns null if no such key exists.
     */
    public String getPrevPath(Object colId) {
	/*		switch (Const.CURR_PROCESSING_STRAT) {
			case COLUMN_AS_STRING: {*/
	return getPrevPath((String)colId);
	/*			}
			case COLUMN_AS_INT: {
				return getPrevPath(((Integer)colId).intValue());
			}
			default: {
				System.err.println("Encountered unknown processing strategy: "+Const.CURR_PROCESSING_STRAT+" ...exiting!");
				System.exit(1);
			}
		}*/
	// 		return null;
    }

    /**
     * Determines the path preceding the local peer's path while preserving prefix hashKey(colId).
     * Returns null if no such key exists.
     */
    public String getPrevPath(String colId) {
	return getPrevPathForPrefix(hashKey(colId).length());
    }

    /**
     * Determines the path preceding the local peer's path while preserving prefix hashKey(colId).
     * Returns null if no such key exists.
     */
    public String getPrevPath(int colId) {
	return getPrevPathForPrefix(hashKey(colId).length());
    }

    /**
     * Determines the next possible key following path. toKey denotes the maximum possible key.
     * Returns null if no such key exists. The returned String is of length Const.MAXBITS.
     * 
     * @deprecated
     */
    public String getNextPath(int toKey, String path) {
	System.err.println("SimpleHashing.getNextPath(int,String) is not finally reengineered!!");
	while (path.length() < MAXBITS) path += '1';
	//		if (Utils.toInt(path) >= toKey) return null;
	if (Integer.parseInt(path,2) >= toKey) return null;
	int idx = path.lastIndexOf('0');
	if (idx >= 0) {
	    path = path.substring(0,idx)+'1';
	    while (path.length() < MAXBITS)
		path += '0';
	} else return null;
	return path;
    }

    /**
     * Determines the key previous to path. toKey denotes the minimum possible key.
     * Returns null if no such key exists. The returned String is of length Const.MAXBITS.
     * 
     * @deprecated
     */
    public String getPrevPath(int toKey, String path) {
	System.err.println("SimpleHashing.getNextPath(int) is not finally reengineered!!");
	while (path.length() < MAXBITS) path += '0';
	//		if (Utils.toInt(path) <= toKey) return null;
	if (Integer.parseInt(path,2) <= toKey) return null;
	int idx = path.lastIndexOf('1');
	if (idx >= 0) {
	    path = path.substring(0,idx)+'0';
	    while (path.length() < MAXBITS)
		path += '1';
	} else return null;
	return path;
    }

    /**
     * Determines the maximum possible key whith prefix hashKey(colId).
     * 
     * @deprecated
     */
    public int getMaxKey(Object colId) {
	/*		switch (Const.CURR_PROCESSING_STRAT) {
			case COLUMN_AS_STRING: {*/
	return getMaxKey((String)colId);
	/*			}
			case COLUMN_AS_INT: {
				return getMaxKey(((Integer)colId).intValue());
			}
			default: {
				System.err.println("Encountered unknown processing strategy: "+Const.CURR_PROCESSING_STRAT+" ...exiting!");
				System.exit(1);
			}
		}*/
	// 		return -1;
    }

    /**
     * Determines the maximum possible key whith prefix hashKey(colId). The returned value corresponds
     * to the decimal value of the binary key k=hashKey(colId)+suffix, where suffix consists of only '1' and
     * k has length Const.MAXBITS.
     * 
     * @deprecated
     */
    public int getMaxKey(String colId) {
	String max = hashKey(colId);
	while (max.length() < MAXBITS) max += '1';
	return Integer.parseInt(max,2);
    }

    /**
     * Determines the maximum possible key whith prefix hashKey(colId). The returned value corresponds
     * to the decimal value of the binary key k=hashKey(colId)+suffix, where suffix consists of only '1' and
     * k has the length Const.MAXBITS.
     * 
     * @deprecated
     */
    public int getMaxKey(int colId) {
	System.err.println("SimpleHashing.getMaxKey(Object) is not finally reengineered!!");
	int colNr = ((Integer)colId).intValue();
	return (colNr + 1 << MAXBITS - COLUMN_BITS) - 1;
    }


    /**
     * compute hash coordinates
     * 
     * @param item
     * @return
     */
    protected int[] getHashCoordinates(String[] item){
	int[] hashCoordinates = new int[item.length];

	for (int i=0;i<item.length;i++){
	    int hashValue = Math.abs(item[i].hashCode());
	    double scaledDouble = (double)hashValue / (double)Integer.MAX_VALUE * (getDimSpecMax()[i] - getDimSpecMin()[i]) + getDimSpecMin()[i];

	    //			/*if (binHashValue.equals("0"))*/ System.err.println("val: "+item[i]+", key unscaled: "+hashValue+", key scaled: "+scaledDouble);
	    hashCoordinates[i] = (int)scaledDouble;
	}

	return hashCoordinates;
    }

    protected int getIntValue(String s) {
    	if (31 > s.length()) return Integer.parseInt(s, 2);
    	return Integer.parseInt(s.substring(0, 31), 2);
    }

    long getLongValue(String s) {
    	return Long.parseLong(s, 2);
    }
}
