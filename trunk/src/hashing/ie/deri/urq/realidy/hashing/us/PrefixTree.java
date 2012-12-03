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

// package pgrid.similarity.util.hashing;
// 
// import pgrid.Constants;
// import pgrid.Properties;
// import pgrid.interfaces.basic.PGridP2P;
// import pgrid.util.Tokenizer;
// import unistore.api.UniStoreImpl;
// import unistore.api.util.UniStoreProperties;
// 

package ie.deri.urq.realidy.hashing.us;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.Vector;


/**
 * This class is responsible for building the indexing structure (the Prefix
 * tree) to map ASCII string to binary strings.
 *
 * @author <a href="mailto:Roman Schmidt <Roman.Schmidt@epfl.ch>">Roman Schmidt</a>
 * @version 1.0.0
 */
public class PrefixTree {
	private final static String classLocaltion = "/ie/deri/urq/realidy/hashing/us/";
	public static final String LINE_SEPERATOR = System.getProperty("line.separator");
	public static final String TREE_INI_FILE="hash.ini";
	public static final String TREE_DB_FILE="hash.dat";

	/**
	 * The standard maximum number of data items per leaf.
	 */
	//	private static int MAX_LEAF_SIZE = 10; //this has effect on the depth of the tree, i.e., the length of the keys
	private static int MAX_LEAF_SIZE = 1; //the smaller value results in less clustering - but, surprisingly first tests let the more clustered tree emerge better...

	/**
	 * The standard length of data strings.
	 */
	//	private static int STRING_LEN = 13; //this has effect on the depth of the tree, i.e., the length of the keys
	private static int STRING_LEN = 32; //the larger value results in less clustering - but, surprisingly first tests let the more clustered tree emerge better...

	/**
	 * The database of traced search strings.
	 */
	private String[] mDatabase = null;

	/**
	 * The maximum number of data items mapped to a leaf.
	 */
	private int mMaxLeafSize = MAX_LEAF_SIZE;

	/**
	 * The length of the PGridServer data strings (negative if variable).
	 */

	/**
	 * The length of the PGridServer data strings (negative if variable).
	 */
	private int mStringLen = STRING_LEN;

	/**
	 * The root tree node.
	 */
	private TreeNode mTreeRoot = null;

	private int mNodeNr = 0;

	private int mDepth = 0;

	/**
	 * Creates a new PGridP2P tree.
	 */
	public PrefixTree() {

	}

	/**
	 * Returns the binary representation for the given query string.
	 *
	 * @param query the query string
	 * @return the binary representation.
	 */
	public String findKey(String query) {
		if (mTreeRoot == null)
			return null;
		String key = findKey(query.toLowerCase(), mTreeRoot);
		//		if (query.equals(new String("<http://www.w3.org/People/Berners-Lee/card#i>")))
		//			System.out.println("==>looking up:"+query+", key is:"+key+",length:"+key.length()+", as int:"+Integer.parseInt(key, 2));
		return key;
	}

	/**
	 * Returns the binary represantation for the given query string, starting at
	 * the given entry node of the PGridP2P tree and recursivly parses throw down
	 * the tree.
	 *
	 * @param query the query string
	 * @param node  the entry node of the PGridP2P tree.
	 * @return the binary represantation.
	 */
	private String findKey(String query, TreeNode node) {
		if ((node.getLeftChild() == null) && (node.getRightChild() == null))
			return "";
		if (node.getPrefix().startsWith(query)) return "";

		//		if (query.equals(new String("<http://www.w3.org/People/Berners-Lee/card#i>").toLowerCase()))
		//		System.out.println("compare("+node.getPrefix()+","+query+")="+node.getPrefix().compareTo(query));

		if (node.getPrefix().compareTo(query) > 0)
			return ("0" + findKey(query, node.getLeftChild()));
		else
			return ("1" + findKey(query, node.getRightChild()));
	}

	/**
	 * Finds the position to split the range.
	 * The first different character at position <code>common</code> in the
	 * neighbouring data strings is searched, starting at the middle of the range
	 * and step apart.
	 *
	 * @param start  the starting index for the subtree.
	 * @param end    the ending index for the subtree.
	 * @param common the length of the common prefix of all the PGridServer data words.
	 * @return the found pivot position to split the range.
	 */
	private int findPivotPos(int start, int end, int common) {
		if (start == end)
			return start;
		while (true) {
			int left = (start + end) / 2;
			int right = (start + end + 1) / 2;
			boolean leftFlag = false;
			boolean rightFlag = false;
			while ((left >= start) && (right <= end)) {
				leftFlag = false;
				rightFlag = false;
				if ((mDatabase[right].length() >= (common + 1)) && (mDatabase[right - 1].length() >= (common + 1)))
					if (mDatabase[right].charAt(common) == mDatabase[right - 1].charAt(common))
						rightFlag = true;
				if ((mDatabase[left].length() >= (common + 1)) && (mDatabase[left + 1].length() >= (common + 1)))
					if (mDatabase[left].charAt(common) == mDatabase[left + 1].charAt(common))
						leftFlag = true;
				if ((!rightFlag) || (!leftFlag))
					break;
				left--;
				right++;
			}
			if (!rightFlag)
				return right;
			if (!leftFlag)
				return left + 1;
			common++;
		}
	}

	/**
	 * Finds the common prefix for two strings.
	 *
	 * @param left   the first string.
	 * @param right  the second string.
	 * @param common the length of prefix guaranteed to be common.
	 * @return the found common prefix.
	 */
	private String findPrefix(String left, String right, int common) {
		int len = Math.min(left.length(), right.length());
		int i;
		for (i = common; i < len; i++)
			if (left.charAt(i) != right.charAt(i))
				break;
		if (i+1 > left.length()) return left;
		return right.substring(0, i + 1); // i+1
	}

	/**
	 * Initializes the PGridP2P tree.
	 */
	synchronized public void init() {
		URL ini = PrefixTree.class.getResource(classLocaltion+TREE_INI_FILE);
		URL db = PrefixTree.class.getResource(classLocaltion+TREE_DB_FILE);

		System.out.println("Tree.ini: "+ini);
		System.out.println("Tree.db: "+db);
		try {
			if(ini!=null){
				InputStream in = PrefixTree.class.getResourceAsStream(classLocaltion+TREE_INI_FILE);
				BufferedReader reader = new BufferedReader(new InputStreamReader(in));
				mTreeRoot = loadTreeFromFile(reader, TREE_INI_FILE);
			}else if(db!=null){
				initWithDbFile();
			}else{
				System.err.println("Neither the "+TREE_DB_FILE+" nor the "+TREE_INI_FILE+" file are available!");
				System.exit(-1);
			}
		} catch (NullPointerException e) {
			System.out.println("Severe: Could not read/write PGrid Tree initialization file '" + TREE_INI_FILE + "'!:"+ e);
			System.exit(-1);
		}
	}
	
	@Override
	public String toString() {
		return "PrefixTree[db:"+getDatabaseSize()+" nodes:"+getNodesCount()+" leaves: "+getLeavesCount()+"]";
	}

	/**
	 * Initializes the PGridP2P tree with a file of traced search queries.
	 */
	synchronized private void initWithDbFile() {
		initWithDbFile(false);
	}

	synchronized private void initWithDbFile(boolean printTree) {
		initWithDbFile(TREE_DB_FILE,printTree);
	}

	synchronized private void initWithDbFile(String fileName, boolean printTree) {
		InputStream in = PrefixTree.class.getResourceAsStream(classLocaltion + fileName);
		Set<String> database = new HashSet<String>();
		try {
			//		    in = new FileInputStream(new File(classLocaltion + fileName));

			if (in == null) {
				System.out.println("Severe: Database file '" + fileName + "' for the PrefixTree does not exist!");
				System.exit(-1);
			}


			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			String line;
			int count =0;
			while ((line = reader.readLine()) != null) {
				count++;
				String[] words = tokenize(line, " \n\r\t");
				for (int i = 0; i < words.length; i++) {
					if (mStringLen < 0)
						database.add(words[i].toLowerCase());
					else if (words[i].length() >= mStringLen)
						database.add(words[i].substring(0, mStringLen).toLowerCase());
				}
				if(count%10000==0)System.out.println("Parsed "+count+" lines");
			}
		} catch (IOException e) {
			System.out.println("Severe: Could not read database file '" + TREE_DB_FILE + "'!:"+ e);
			System.exit(-1);
		}

		// eliminate duplicate entries
//		int index = 0;
//		for(String db: database )
//		while (index < (database.size() - 1)) {
//			if (((String)database.get(index)).toLowerCase().equals(((String)database.get(index + 1)).toLowerCase()))
//				database.remove(index + 1);
//			else
//				index++;
//		}

		// copy to Array
		TreeSet<String> s = new TreeSet<String>();
		s.addAll(database);
		
		mDatabase = new String[database.size()];
		mDatabase = s.toArray(mDatabase);
//		for (Strinint i = 0; i < database.size(); i++) {
//			mDatabase[i] = ((String)database.get(i)).toLowerCase();
//		}
		System.err.println("build db: "+mDatabase.length);

//		// sort data
//		Arrays.sort(mDatabase);

		if (mDatabase.length == 0) {
			System.out.println("Warning: Database '" +  TREE_DB_FILE + "' has no words with required length!");
		}
		System.err.println("makeTreeNode");
		// create the tree
		mTreeRoot = makeTreeNode(0, mDatabase.length - 1, 0);

		// saveDataTable the tree to a file
		File file = new File(TREE_INI_FILE);
		try {
			FileWriter fWriter = new FileWriter(file);
			saveTreeToFile(fWriter, mTreeRoot, file.getName());
			fWriter.close();
			System.out.println("Attention! Wrote PrefixTree ini file to: "+file.getAbsolutePath());
		} catch (IOException e) {
			System.out.println("Severe: Could not create PGrid Tree initialization file '" + file.getName() + "'!:"+ e);
			System.exit(-1);
		}

		if (printTree) printTree(mTreeRoot,0);
		System.out.println("depth: "+getDepth());
	}

	/**
	 * Initializes the PGridP2P Tree with PGridP2P Tree initialization file.
	 *
	 * @param reader   the PGridP2P Tree initialization file.
	 * @param filename the initialization filename.
	 * @return the created PGridP2P Tree node.
	 */
	private TreeNode loadTreeFromFile(BufferedReader reader, String filename) {
		String prefix;
		try {
			prefix = reader.readLine();
		} catch (IOException e) {
			System.out.println("Warning: Could not read/write PGrid Tree initialization file '" + filename + "'!:"+ e);
			return null;
		}
		if (prefix.equals(""))
			return null;
		TreeNode leftChild = loadTreeFromFile(reader, filename);
		TreeNode rightChild = loadTreeFromFile(reader, filename);

		int leaves = (leftChild == null ? 0 : leftChild.getLeavesCount());
		leaves += (rightChild == null ? 0 : rightChild.getLeavesCount());
		int leftDepth = (leftChild == null ? 0 : leftChild.getDepth());
		int rightDepth = (rightChild == null ? 0 : rightChild.getDepth());
		int depth = Math.max(leftDepth, rightDepth);
		int nodes = 1;
		nodes += (leftChild == null ? 0 : leftChild.getNodesCount());
		nodes += (rightChild == null ? 0 : rightChild.getNodesCount());

		// depth is somehow not set correctly, so I added a small "hack"
		//		return new TreeNode(prefix, leftChild, rightChild, leaves, depth, nodes, ++mNodeNr);
		TreeNode root = new TreeNode(prefix, leftChild, rightChild, leaves, depth, nodes, ++mNodeNr);
		mDepth=getDepth(root,1);
		return root;
	}

	/**
	 * Creates a PGridP2P tree node and recursivly calls this function to create the
	 * child nodes for this node.
	 *
	 * @param start  the starting index for the subtree.
	 * @param end    the ending index for the subtree.
	 * @param common the length of the common prefix of all the PGridServer data words.
	 * @return the created PGridP2P tree node.
	 */
	private TreeNode makeTreeNode(int start, int end, int common) {
		//		System.out.println("makeTreeNode("+start+","+end+","+common+")");
		if (((end - start + 1) <= mMaxLeafSize) || (start == end)) {
			//			System.out.println("\tnew TreeNode("+mDatabase[(start + end) / 2]+",null,null,1,1,1,"+(mNodeNr+1)+")");
			return new TreeNode(mDatabase[(start + end) / 2], null, null, 1, 1, 1, ++mNodeNr);
		} else {
			int pivotPos = findPivotPos(start, end, common);
			//			System.out.println("\tpivot: "+pivotPos);
			String prefix = findPrefix(mDatabase[pivotPos - 1], mDatabase[pivotPos], common);
			//			System.out.println("\tfindPrefix("+mDatabase[pivotPos - 1]+","+mDatabase[pivotPos]+","+common+")="+prefix);

			common = prefix.length() - 1;
			TreeNode leftChild = makeTreeNode(start, pivotPos - 1, common);
			TreeNode rightChild = makeTreeNode(pivotPos, end, common);
			int leaves = leftChild.getLeavesCount() + rightChild.getLeavesCount();
			int depth = Math.max(leftChild.getDepth(), rightChild.getDepth());
			int nodes = leftChild.getNodesCount() + rightChild.getNodesCount() + 1;

			//			System.out.println("\tnew TreeNode("+prefix+","+leftChild+","+rightChild+","+leaves+","+depth+","+nodes+","+(mNodeNr+1)+")");
			return new TreeNode(prefix, leftChild, rightChild, leaves, depth, nodes, ++mNodeNr);
		}
	}

	/**
	 * Returns the number of leaves in the PGridP2P tree.
	 *
	 * @return the number of leaves in the PGridP2P tree.
	 */
	int getLeavesCount() {
		return mTreeRoot.getLeavesCount();
	}

	/**
	 * Returns the used database.
	 *
	 * @return the used database.
	 */
	String[] getDatabase() {
		return mDatabase;
	}

	/**
	 * Returns the size of the used database.
	 *
	 * @return the size of the used database.
	 */
	int getDatabaseSize() {
		return mDatabase.length;
	}

	/**
	 * Returns the depth of the PGridP2P tree.
	 *
	 * @return the depth of the PGridP2P tree.
	 */
	int getDepth() {
		// somehow not set correctly
		//		return mTreeRoot.getDepth();
		// too expensive each time
		//		return getDepth(mTreeRoot,1);
		// so added a small hack in loadTreeFromFile()
		return getDepth(mTreeRoot,1);

	}

	int getDepth(TreeNode node, int depth) {
		TreeNode left = node.getLeftChild();
		TreeNode right = node.getRightChild();
		//		int depth = node.getDepth();
		++depth;
		int leftSubTreeDepth = 0;
		int rightSubTreeDepth = 0;
		if (left != null) leftSubTreeDepth=getDepth(left,depth);
		if (right != null) rightSubTreeDepth=getDepth(right,depth);
		if (leftSubTreeDepth > depth) depth = leftSubTreeDepth;
		if (rightSubTreeDepth > depth) depth = rightSubTreeDepth;
		return depth;
	}

	/**
	 * Returns the number of nodes in the PGridP2P tree.
	 *
	 * @return the number of nodes in the PGridP2P tree.
	 */
	int getNodesCount() {
		return mTreeRoot.getNodesCount();
	}

	public void createKeys(String filename) {
		File file = new File(filename);
		try {
			FileWriter fWriter = new FileWriter(file);
			createKeys(fWriter, mTreeRoot, filename, "");
			fWriter.close();
		} catch (IOException e) {
			System.out.println("Warning: Could not write to file '" + filename + "'!: "+ e);
		}
	}

	void createKeys(FileWriter writer, TreeNode node, String filename, String key) {
		try {
			TreeNode left = node.getLeftChild();
			TreeNode right = node.getRightChild();
			if (left != null)
				createKeys(writer, left, filename, key + "0");
			if (right != null)
				createKeys(writer, right, filename, key + "1");
			if ((left == null) && (right == null))
				writer.write(node.getPrefix() + LINE_SEPERATOR);
		} catch (IOException e) {
			System.out.println("Warning: Could not write to file '" + filename + "'!:" + e);
		}
	}

	/**
	 * Saves the PGridP2P Tree to a file.
	 * This information can be used to reconstruct the tree.
	 *
	 * @param writer   the file writer to store the tree.
	 * @param node     the node to store.
	 * @param filename the filename.
	 */
	private void saveTreeToFile(FileWriter writer, TreeNode node, String filename) {
		try {
			writer.write(node.getPrefix() + LINE_SEPERATOR);
			TreeNode left = node.getLeftChild();
			TreeNode right = node.getRightChild();
			if (left != null)
				saveTreeToFile(writer, left, filename);
			else
				writer.write(LINE_SEPERATOR);
			if (right != null)
				saveTreeToFile(writer, right, filename);
			else
				writer.write(LINE_SEPERATOR);
		} catch (IOException e) {
			System.out.println("Severe: Could not read/write PGrid Tree initialization file '" + filename +"'!:"+ e);
			System.exit(-1);
		}
	}

	public void printTree()
	{
		printTree(mTreeRoot,0);
	}

	private void printTree(TreeNode node,int tabCnt) {
		for (int i = 0; i < tabCnt-1; ++i)
		{
			if (i % 2 == 0) System.out.print("|");
			else System.out.print(" ");
		}
		if (0 < tabCnt) System.out.print(">");
		System.out.println(node.toString()+":"+node.getPrefix());
		TreeNode left = node.getLeftChild();
		TreeNode right = node.getRightChild();
		if (left != null)
			printTree(left,tabCnt+2);
		//		else
		//		writer.write(LINE_SEPERATOR);
		if (right != null)
			printTree(right,tabCnt+2);
		//		else
		//		writer.write(LINE_SEPERATOR);
	}

	/**
	 * This inner class represents a node of the PGridP2P tree.
	 */
	private class TreeNode {

		/**
		 * The greater depth of the subtrees.
		 */
		private int mDepth = 0;

		/**
		 * The number of leaves in the subtrees.
		 */
		private int mLeavesCount = 0;

		/**
		 * The left subtree.
		 */
		private TreeNode mLeftChild = null;

		/**
		 * The number of nodes in the subtrees.
		 */
		private int mNodesCount = 0;

		/**
		 * The prefix of this node.
		 */
		private String mPrefix = new String();

		/**
		 * The right subtree.
		 */
		private TreeNode mRightChild = null;

		private int mNr;

		/**
		 * Creates a new tree node with the given properties.
		 *
		 * @param prefix      the prefix for this node.
		 * @param leftChild   the left subtree
		 * @param rightChild  the right subtree
		 * @param leavesCount the number of leaves in the subtrees.
		 * @param depth       the greater depth of the subtrees.
		 * @param nodesCount  the number of nodes in the subtrees.
		 */
		private TreeNode(String prefix, TreeNode leftChild, TreeNode rightChild, int leavesCount, int depth, int nodesCount,int nr) {
			mPrefix = prefix;
			mLeftChild = leftChild;
			mRightChild = rightChild;
			mLeavesCount = leavesCount;
			mDepth = depth;
			mNodesCount = nodesCount;
			mNr = nr;
		}

		/**
		 * Returns the greater of depth of the subtrees.
		 *
		 * @return the greater of depth of the subtrees.
		 */
		private int getDepth() {
			return mDepth;
		}

		/**
		 * Returns the number of leaves in the subtrees.
		 *
		 * @return the number of leaves in the subtrees.
		 */
		private int getLeavesCount() {
			return mLeavesCount;
		}

		/**
		 * Returns the left subtree.
		 *
		 * @return the left subtree.
		 */
		private TreeNode getLeftChild() {
			return mLeftChild;
		}

		/**
		 * Returns the number of nodes in the subtrees.
		 *
		 * @return the number of nodes in the subtrees.
		 */
		private int getNodesCount() {
			return mNodesCount;
		}

		/**
		 * Returns the prefix for this node.
		 *
		 * @return the prefix for this node.
		 */
		private String getPrefix() {
			return mPrefix;
		}

		/**
		 * Returns the right subtree.
		 *
		 * @return the right subtree.
		 */
		private TreeNode getRightChild() {
			return mRightChild;
		}

		public String toString()
		{
			return Integer.toString(mNr);
		}

	}
	public static String[] tokenize(String input, String separator) {
		Vector vector = new Vector();
		StringTokenizer strTokens = new StringTokenizer(input, separator);
		String[] strings;

		while (strTokens.hasMoreTokens())
			vector.addElement(strTokens.nextToken());
		strings = new String[vector.size()];
		for (int i = 0; i < strings.length; i++)
			strings[i] = (String)vector.get(i);
		return strings;
	}

}