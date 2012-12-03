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
// import pgrid.similarity.Const;
// import pgrid.similarity.Utils;
// import unistore.api.*;
// import unistore.api.util.*;

public class MarioHashing extends SimpleHashing
{
    public MarioHashing(String hasherPrefix, int[] dimSpecMin, int[] dimSpecMax) {
	super(hasherPrefix,dimSpecMin,dimSpecMax);
    }

    /**
     * returns the hashKey for the given objects
     */
    public String hashKey(int oid)
    {
	String result = Integer.toBinaryString(oid);
	while (result.length() <MAXBITS) result = '0' + result;
	return result;
    }

    public String hashKey(Object o, int col){
	/*		if (Const.CURR_PROCESSING_STRAT != Const.CURR_PROCESSING_STRAT.COLUMN_AS_INT)
		{
			System.err.println("Warning! Called MarioHashing.hashkey(Object,int), but processing strategy is set to: "+Const.CURR_PROCESSING_STRAT);
			return hashKey(o,"COL"+col);
		}*/
	String result = Integer.toBinaryString(col);
	while (result.length() < COLUMN_BITS) result = '0' + result;
	String res1 = hashKey(o,true,MAXBITS - COLUMN_BITS);

	return result + res1;
    }

    public String hashKey(Object o, String col){
	/*		if (Const.CURR_PROCESSING_STRAT != Const.CURR_PROCESSING_STRAT.COLUMN_AS_STRING)
		{
			System.err.println("Warning! Called MarioHashing.hashkey(Object,String), but processing strategy is set to: "+Const.CURR_PROCESSING_STRAT);
			return hashKey(o,Integer.MAX_VALUE);
		}*/
	String result = hashKey(col,false,0);
	System.err.println("hashkey of column title '"+col+"': "+result+", len: "+result.length()+" (="+toInt(result)+") prefix!");

	String res1 = hashKey(o,true,MAXBITS - result.length());
	System.err.println("hashkey of column value '"+o+"': "+res1+", len: "+res1.length()+" (="+toInt(res1)+") suffix!");

	return result + res1;
    }

    public String hashKey(Object o)
    {
	return hashKey(o,false,0);
    }

    private String hashKey(Object o, boolean respectLen, int len)
    {
	String res = null;
	if (o instanceof Integer)
	{
	    res = Integer.toBinaryString(Integer.parseInt(o.toString()));
	    if (respectLen)
	    {
		while (res.length() < len) res = '0' + res;
	    }
	}
	else
	{
	    String s = o.toString();
	    int zahl = 0;
	    int _27 = 1;
	    for (int i = s.length() - 1;i >= 0;i--){
		Character a = s.charAt(i);
		if (a == ' ') zahl += 26 * _27;
		else zahl += (a.hashCode() - 97) * _27;
		_27 *= 27;
	    }
	    int laenge = (int)(Math.round(s.length() * Math.log(27) / Math.log(2)));
	    res = Integer.toBinaryString(zahl);
	    while (res.length() < laenge) res = '0' + res;
	    if (respectLen)
	    {
		if (res.length() < len) System.err.println("!! Filling hashkey with "+(len-res.length())+" 0...");
		while (res.length() < len) res += '0';
	    }
	}
	return res;
    }

    /**
     * transforms a binary string into an integer
     */
    public static int toInt(String s){
	int zwei = 1;
	int result = 0;
	while (s.length() < MAXBITS) s += '0'; 
	for (int i = s.length() - 1;i > -1;i--){
	    result = (s.charAt(i) == '0' ? result : result + zwei);
	    zwei *= 2;
	}
	return result;
    }

	@Override
	public int objectHash(Node s) {
		return toInt(hashKey(s.toString()));
	}

	@Override
	public int predicateHash(Node s) {
		return toInt(hashKey(s.toString()));
	}

	@Override
	public int subjectHash(Node s) {
		return toInt(hashKey(s.toString()));
	}
}
