package de.ilmenau.datasum.index;

import javax.swing.JTree;
import javax.swing.tree.DefaultMutableTreeNode;

/**
 * This class represents a local index.
 * 
 * @author Katja Hose
 * @version $Id: LocalIndex.java,v 1.6 2007-02-13 17:15:09 hose Exp $
 */
public abstract class LocalIndex extends Index {
	private static final long serialVersionUID = 1L;
	/**
	 * constructor.
	 * 
	 * @param type the index type
	 */
	public LocalIndex(IndexType type) {
		super(type);
	}

	/**
	 * method that creates an empty index out of itself for the given neigbor.
	 * (and setting initial characteristics)
	 * 
	 * @param neighbor the correspondig neighbor
	 * @return an empty LocalIndex with initial characteristics
	 */
	//public abstract LocalIndex createEmptyLocalIndexForNeighbor(Neighbor neighbor);

	/**
	 * abstract method that determines if this Index object contains any relevant data matching any
	 * String contained in the input Vector <em>simplifiedExp</em>.
	 * 
	 * @param simplifiedExp representing XPath expressions simplified to regular expressions
	 * 
	 * @return <tt>true</tt> when the index data does specify relevant data
	 */
	public abstract boolean hasAccordingData(String simplifiedExp);
	
	/**
	 * method that builds a subtree of DefaultMutableTreeNodes with 
	 * the given <em>parentNode</em> as root node. This is used to build a JTree for displaying
	 * index information to the user.
	 * 
	 * @param parentNode the root node of the whole subtree
	 */
	public abstract void buildJSubTree(DefaultMutableTreeNode parentNode);
	
	
	/**
	 * method that creates a JTree describing the data of this index object
	 * 
	 * @param rootText name of the node that forms the root of the created tree
	 */
	public JTree createJTree(String rootText){
		
		rootText += " (" + this.getType().name() + ")";
	  	
	    DefaultMutableTreeNode root = new DefaultMutableTreeNode(rootText);
	    
	    this.buildJSubTree(root);
	    
	    // create JTRee
	    JTree treeStructure = new JTree(root);
		
		return treeStructure;
	}
	
	
	/**
	 * method that creates a tree consisting of DefaultMutableTreeNodes. 
	 * The method creates a node with <em>rootText</em> as name and 
	 * adds that node as child of <em>rootNode</em>.
	 * 
	 * @param rootText name of the node that is to be created
	 * @param rootNode parent of the resulting subtree
	 */
	public void buildJSubTreeWithRoot(String rootText, DefaultMutableTreeNode rootNode){
		
		rootText += " (" + this.getType().name() + ")";
	  	
	    DefaultMutableTreeNode root = new DefaultMutableTreeNode(rootText);
	    rootNode.add(root);
	    
	    this.buildJSubTree(root);
	    
	}
	
	
}
