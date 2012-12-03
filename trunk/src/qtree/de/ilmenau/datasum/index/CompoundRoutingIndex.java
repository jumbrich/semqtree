package de.ilmenau.datasum.index;

import de.ilmenau.datasum.exception.NotYetImplementedException;
import de.ilmenau.datasum.index.qtree.QRoutingIndex;

/**
 * This class represents a Compound Routing Index.
 * It is an role model (abstract) for the concrete Index implementations as well as a Factory that
 * chooses the appropiate subclass 
 * 
 * @author Katja Hose
 */
public abstract class CompoundRoutingIndex extends Index {
	private static final long serialVersionUID = 1L;
	/**
	 * method that creates an empty index object.
	 * 
	 * @param type the index type
	 * @return the fresh index
	 */
	@SuppressWarnings("nls")
	public static CompoundRoutingIndex getEmptyIndexObject(IndexType type) {
		/*if (type == IndexType.ElementCRI || type == IndexType.PathCRI) {
			return new XPathRoutingIndex(type);
		} else*/ if (type == IndexType.QTree) {
			return new QRoutingIndex();
		/*} else if (type == IndexType.MultiDimHistogram) {
			return new MultiDimRoutingHistogram();*/
		} else {
			throw new NotYetImplementedException(
				"Error while initializing neighbor objects with empty indexes."
				+ " The indicated index type '" + type + "' is not supported."
			);
		}
	}

	/**
	 * constructor.
	 * 
	 * @param type the index type
	 */
	public CompoundRoutingIndex(IndexType type) {
		super(type);
	}

	/**
	 * abstract method for adding the index data of another index to the local index information
	 * 
	 * @param neighbor the neighbor
	 * @param inputIndex the corresponding local index
	 */
	//public abstract void addIndexData(Neighbor neighbor, LocalIndex inputIndex);

	
	/**
	 * method that returns the index for the given neighbor.
	 * (necessary if the compound routing index is the same for all neighbors)
	 * 
	 * @param neighbor the neighbor
	 * @return the index object
	 */
	//public abstract LocalIndex getLocalIndexForNeighbor(Neighbor neighbor);
}
