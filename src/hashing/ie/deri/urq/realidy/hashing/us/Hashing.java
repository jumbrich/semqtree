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

/*
 * Created on 15.05.2006
 */
// package pgrid.similarity.util.hashing;

public interface Hashing
{
	/**
	 * @deprecated
	 * 
	 * @param o
	 * @param col
	 * @return
	 */
	public String hashKey(Object o, int col);

	/**
	 * Computes a hashkey of concatenation col#o by concatenating hashKey(col)#hashKey(o).
	 * 
	 * @param o the value to hash
	 * @param col the column name
	 * @return h(col)#h(o)
	 */
	public String hashKey(Object o, String col);
	
	/**
	 * Computes a hashkey of object id oid
	 * 
	 * @param o the value to hash
	 * @param col the column name
	 * @return h(oid)
	 */
	public String hashKey(int oid);

	/**
	 * Computes a hashkey of value o
	 * 
	 * @param o the value to hash
	 * @param col the column name
	 * @return h(o)
	 */
	public String hashKey(Object o);

	/**
	 * Determines the next possible key for the local peer, no prefix is preserved.
	 * Returns null if no such key exists.
	 */
	public String getNextPath();

	/**
	 * Determines the next possible key for the local peer with prefix hashKey(colId).
	 * Returns null if no such key exists.
	 */
	public String getNextPath(Object colId);
	
	/**
	 * Determines the next possible key following path. toKey denotes the maximum possible key.
	 * Returns null if no such key exists.
	 */
	public String getNextPath(int toKey, String path);

	/**
	 * Determines the key with prefix hashKey(colId) previous to the path of the local peer.
	 * Returns null if no such key exists.
	 */
	public String getPrevPath(Object colId);

	/**
	 * Determines the key previous to path. toKey denotes the maximum possible key.
	 * Returns null if no such key exists.
	 */
	public String getPrevPath(int toKey, String path);
	
	/**
	 * Determines the maximum possible key whith prefix hashKey(colId).
	 */
	public int getMaxKey(Object colId);
}
