/**
 * 
 */
package de.ilmenau.datasum.index.qtree;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Vector;


/**
 * @author matz, hose
 * 
 * Own Implementation of a Priority Queue because the one from java.util
 * is not usable
 * Insert,Update,Delete O(n)
 * Top,Pop O(1)
 */

@SuppressWarnings("serial")
public class QPriorityQueue implements Serializable,Cloneable{
	private static final long serialVersionUID = 1L;
	
	private Vector<QTreeNode> elements;
	
	/**
	 * Creates a new QPriorityQueue
	 *
	 */
	public QPriorityQueue() {
		this.elements = new Vector<QTreeNode>();
	}
	
	@SuppressWarnings("unchecked")
	public QPriorityQueue clone(){
		QPriorityQueue clone;
		try {
			clone = (QPriorityQueue) super.clone();
			clone.elements = (Vector<QTreeNode>) this.elements.clone();
		} catch (CloneNotSupportedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			clone=null;
		}
		return clone;	
	}
	
	/**
	 * Adds the given Node to the Priority Queue regarding to its
	 * value of nextToMergePen
	 * 
	 * TODO<MM>: nextToMergePen als Parameter �bergeben 
	 * 
	 * @param qtn
	 */
	public void add(QTreeNode qtn) {
		// Key,Object in Sortierreihenfolge einfuegen
		double k = qtn.nextToMergePen;
		int i;
		if (!(elements.isEmpty())){
			for (i = 0;i < elements.size();i++){
				if (elements.get(i).nextToMergePen > k) break;  
			}
			elements.add(i,qtn);
		}
		else {
			elements.add(qtn);
		}
	}
	
	/**
	 * Removes the Node q from the Queue if it exists
	 * @param q
	 * @return True if <i>q</i> could be removed
	 */
	public boolean remove(QTreeNode q) {
		// Node aus Liste entfernen
		for (QTreeNode cn: elements){
			if (cn == q) {
				elements.remove(cn);
				return true;
			}
		}
		return false;
	}
		
	/**
	 * Returns the Top Element of the Queue and removes it
	 * @return The Top Element of the Queue
	 */
	public QTreeNode poll(){
	 // Oberstes Element(mit niedrigstem Schluessel) zurueckgeben und entfernen
		if (elements.size() == 0) return null;
		QTreeNode ret = elements.elementAt(0); // Element 0 ist immer das oberste
		elements.remove(0);
		return ret;		
	}

	/**
	 * Returns the Top Element of the Queue without removing it
	 * @return The Top Element of the Queue qithout removing
	 */
	public QTreeNode peek() {
		if (elements.size() == 0) return null; // Nix mehr da
		else return elements.elementAt(0);
	}
	
	/**
	 * Checks if the given QTreeNode Element is present in this Queue
	 * @param q The Node that should be checked
	 * @return True if the Queue contains <i>q</i>
	 */
	public boolean contains(QTreeNode q) {
		for (QTreeNode cn: elements){
			if (cn == q) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Rebuilds the Priority Queue by replacing all Nodes stored with
	 * The corresponding new Node associated by the assocTable
	 * 
	 * @param assocTable The Table defining the Associatet Nodes
	 */
	public void rebuild(HashMap<QTreeNode,QTreeNode> assocTable){
		//Vector<QTreeNode> newElementsList = new Vector<QTreeNode>();
		for (int i=0;i<this.elements.size();i++){
			elements.set(i, assocTable.get(elements.get(i)));
		}
	}
}
