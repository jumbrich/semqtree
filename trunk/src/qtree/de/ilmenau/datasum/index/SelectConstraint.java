/*
 * Created: 30.01.2007
 * Changed: $Date$
 * 
 * $Log$
 * Revision 1.2  2007-04-15 13:44:06  lemke
 * + added toString()
 *
 * Revision 1.1  2007/01/30 20:01:51  lemke
 * + initial import
 *
 */
package de.ilmenau.datasum.index;

/**
 * This class represents a constraint used by the SelectPop.
 * 
 * @author Christian Lemke
 * @version $Id$
 */
public final class SelectConstraint {
	private static final long serialVersionUID = 1L;
	/** possible comparison operators */
	public enum Operator {
		/** < */
		SMALLER,
		/** <= */
		SMALLER_OR_EQUAL,
		/** = */
		EQUAL,
		/** >= */
		GREATER_OR_EQUAL,
		/** > */
		GREATER
	}

	/** the index of the element name in the index (attributeName) */
	private int elementIndex;
	/** the value of the corresponding element */
	private int elementValue;
	/** the operator used for the comparison */
	private Operator comparisonOperator;


	/**
	 * constructor.
	 * 
	 * @param elementIndex the index of the element name in the index (attributeName)
	 * @param elementValue the value of the corresponding element
	 * @param comparisonOperator the operator used for the comparison
	 */
	public SelectConstraint(int elementIndex, int elementValue, Operator comparisonOperator) {
		this.elementIndex = elementIndex;
		this.elementValue = elementValue;
		this.comparisonOperator = comparisonOperator;
	}

	/**
	 * @return returns the operator used for the comparison
	 */
	public Operator getComparisonOperator() {
		return this.comparisonOperator;
	}

	/**
	 * @return returns the index of the element name in the index (attributeName)
	 */
	public int getElementIndex() {
		return this.elementIndex;
	}

	/**
	 * @return returns the value of the corresponding element
	 */
	public int getElementValue() {
		return this.elementValue;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@SuppressWarnings("nls")
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("attribute ");
		sb.append(this.elementIndex);
		switch (this.comparisonOperator) {
			case SMALLER:
				sb.append(" < ");
				break;
			case SMALLER_OR_EQUAL:
				sb.append(" <= ");
				break;
			case EQUAL:
				sb.append(" = ");
				break;
			case GREATER_OR_EQUAL:
				sb.append(" >= ");
				break;
			case GREATER:
				sb.append(" > ");
				break;
		}
		sb.append(this.elementValue);
		sb.append("\n");

		return sb.toString();
	}
}
