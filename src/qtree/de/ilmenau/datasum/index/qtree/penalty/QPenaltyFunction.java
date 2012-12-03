/* 
 *
 */
package de.ilmenau.datasum.index.qtree.penalty;

import java.io.Serializable;

import de.ilmenau.datasum.index.Bucket;


/**
 * @author marcel
 * This class represents a Penalty Calculation Function
 */
public abstract class QPenaltyFunction implements Cloneable,Serializable{
	
	/** Stores the "Name" of the Penalty Function*/
	private String ID;
	
	/**
	 * Constructor
	 * @param id The Name of the Penalty Function
	 */
	public QPenaltyFunction(String id) {
		this.ID = id;
	}
	
	/**
	 * Calculates the Penalty of the given Bucket b according to the
	 * Dimspecs given. <br>
	 * <b>Note:</b> Penalty Calculation is defined for one Bucket only. <br>
	 * If you want calculate the Penalty for Grouping/Merging 2 or more Buckets
	 * together, create a temporary "Merged/Grouped" Bucket out of them and then
	 * call this Method.
	 * 
	 * @param dimSpecMin The Lower Boundaries of the Dimension
	 * @param dimSpecMax The Upper Boundaries of the Dimension
	 * @param b - The Bucket from which the calculation should be done
	 * @return The Penalty Value
	 */
	public abstract double calculatePenalty(int[] dimSpecMin, int[] dimSpecMax,
											Bucket b);
	
	/**
	 * @return The Name of the Penalty Function
	 */
	public String getID(){
		return this.ID;
	}
	
	/**
	 * Returns a clone of this QPenaltyFunction
	 * @return The cloned PenaltyFunction
	 */
	public QPenaltyFunction clone() {
		try {
			return (QPenaltyFunction) super.clone();
		} catch (CloneNotSupportedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	
}
