package de.ilmenau.datasum.peer;


import java.io.Serializable;

import de.ilmenau.datasum.index.CompoundRoutingIndex;
import de.ilmenau.datasum.index.Index;
import de.ilmenau.datasum.index.LocalIndex;
import de.ilmenau.datasum.index.update.QuerySpaceCache;

/**
 * This class represents a Neighbor object and therefore encapsulates index data concerning
 * the neighbor specified by neihgborID.
 *
 * @author Katja Hose
 * @version 1.0
 * \date    created on 30.06.2004
 */
@SuppressWarnings( { "nls", "serial" })
public class Neighbor implements Serializable {

	/** specifies the peerID of the neighbor */
	private final String neighborID;
	/** hashCode of the neighbor -> precalculated because of problem in serialization */
	private final int hash;
	/** the peer, this neighbor-object belongs to */
	//private LocalPeerObject localPeer;
	/** index data for the specified neighbor */
	private CompoundRoutingIndex index = null;
	/**
	 * the last time step when updates were sent to this neighbor
	 * (used by QUERY_ESTIMATION update strategy)
	 */
	private transient int lastTimeUpdatesSent = 1;
	/**
	 * a cache with the last query spaces
	 * (used by QUERY_FEEDBACK update strategy)
	 */
	private transient QuerySpaceCache querySpaceCache = null;
	/**
	 * the next time step when the updates of this neighbor will be requested
	 * (used by PROACTIVE update strategy)
	 */
	private transient int nextTimeForRequestingUpdates = 0;


	/**
	 * standard constructor.
	 * 
	 * @param id String that represents the neighbor's peerID
	 * @param localPeer LocalPeerObject that represents the neighbor 
	 * TODO: leads to problems when there are more than one PeerManager
	 */
	/*public Neighbor(String id, LocalPeerObject localPeer) {
		if (id == null) {
			throw new NullPointerException("Neighbor needs an ID!");
		}
		this.hash = id.hashCode();
		this.neighborID = id;
		this.localPeer = localPeer;
	}*/
	
	public Neighbor(String id) {
		if (id == null) {
			throw new NullPointerException("Neighbor needs an ID!");
		}
		this.hash = id.hashCode();
		this.neighborID = id;
		//this.localPeer = localPeer;
	}

	/**
	 * method for adding the input index data to this Neighbor's Index object.
	 * 
	 * @param indexToAdd CompoundRoutingIndex whose data shall be added
	 * @param indexType String that specifies the index type that is being used
	 * 
	 */
	public void addIndexDataToCRI(LocalIndex indexToAdd, Index.IndexType indexType) {
		if (this.index == null) {
			this.index = CompoundRoutingIndex.getEmptyIndexObject(indexType);
		}
		//this.index.addIndexData(this, indexToAdd);
	}

	/**
	 * method that aggregate updates in the routing index if necessary.
	 * (create and set updates as BucketAURList -- ADD/UPDATE/REMOVE buckets.)
	 * The recent updates (data item and/or buckets) will be replaced by the changed buckets.
	 *  
	 * @param theNeighborID for which neighbor to aggregate the updates 
	 */
	public void aggregateUpdates(String theNeighborID) {
		/*if (this.index instanceof BucketBasedCompoundRoutingIndex) {
			LocalIndex localIndex = this.index.getLocalIndexForNeighbor(this);
			((BucketBasedLocalIndex) localIndex).aggregateUpdates(theNeighborID);
		}*/
	}

	/**
	 * method that computes the change information of this neighbor.
	 * 
	 * @see BucketBasedLocalIndex#computeAllChangeInformations()
	 * @return vector of the change information
	 */
	/*public HashMap<String, IndexChangeInformation> computeNeighborAllChangeInformations() {
		if (this.index instanceof BucketBasedCompoundRoutingIndex) {
			LocalIndex localIndex = this.index.getLocalIndexForNeighbor(this);
			return ((BucketBasedLocalIndex) localIndex).computeAllChangeInformations();
		}
		throw new NotYetImplementedException(
			"Neighbor.computeNeighborChangeInformations only supported for bucket based indexes!"
		);
	}*/

	/**
	 * method that computes the change information of this neighbor.
	 * 
	 * @param neighborIDsToConsider for which neighbors to calculate the change information 
	 * @return vector of the change information
	 */
	/*public Vector<IndexChangeInformation> computeNeighborChangeInformations(Vector<String> neighborIDsToConsider) {
		if (this.index instanceof BucketBasedCompoundRoutingIndex) {
			LocalIndex localIndex = this.index.getLocalIndexForNeighbor(this);
			return ((BucketBasedLocalIndex) localIndex).computeChangeInformations(neighborIDsToConsider);
		}
		throw new NotYetImplementedException(
			"Neighbor.computeNeighborChangeInformations only supported for bucket based indexes!"
		);
	}*/

	/**
	 * method that creates an empty index out of itself for the this neigbor.
	 * (and setting initial characteristics)
	 * 
	 * @return an empty LocalIndex with initial characteristics
	 */
	/*public LocalIndex createEmptyLocalIndex() {
		return this.localPeer.getLRI().createEmptyLocalIndexForNeighbor(this);
	}*/

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (!(obj instanceof Neighbor)) {
			return false;
		}
		Neighbor n = (Neighbor) obj;
		return this.neighborID.equals(n.neighborID);
	}

	/**
	 * @return returns lastTimeUpdatesSent
	 */
	public int getLastTimeUpdatesSent() {
		return this.lastTimeUpdatesSent;
	}

	/**
	 * @return the compound routing index associated with that neighbor
	 */
	/*public LocalIndex getLocalIndex() {
		return this.index.getLocalIndexForNeighbor(this);
	}*/

	/**
	 * method that returns the local peer object which contains this neighbor object. 
	 * 
	 * @return the local peer object
	 */
	/*public LocalPeerObject getCorrespondingPeerObject() {
		return this.localPeer;
	}*/

	/**
	 * method that returns the ID of the peer that is represented by this neighbor object.
	 * 
	 * @return String that represents this Neighbor object's peerID
	 */
	public String getNeighborID() {
		return this.neighborID;
	}

	/**
	 * @return returns nextTimeForRequestingUpdates
	 */
	public int getNextTimeForRequestingUpdates() {
		return this.nextTimeForRequestingUpdates;
	}

	/**
	 * method that returns the ID of the peer which contains this neighbor object. 
	 * 
	 * @return the ID
	 */
	public String getPeerID() {
		//return this.localPeer.getPeerID();
		return "";
	}

	/**
	 * @return returns querySpaceCache
	 */
	public QuerySpaceCache getQuerySpaceCache() {
		return this.querySpaceCache;
	}

	/**
	 * method for getting this Neighour object's state String that contains index information. 
	 *
	 * @return String that represents this object's state String.
	 */
	/*public String getStateString() {
		String returnString = new String();
		if (this.index != null) {
			// Typunterscheidung
			if (this.localPeer.getIndexType().equals(Index.IndexType.QTree)) {
				returnString += ((QRoutingIndex) this.index).getStateStringForNeighbor(this);
			} else
				returnString += this.index.getStateString();
		}
		return returnString;
	}*/

	/**
	 * method for checking if this Neighbor's index contains data corresponding to the input values.
	 * Vector costObjects is an empty Vector. After completion of method, it contains costObjects, 
	 * costObjects[i] corresponds to simplifiedExp[i].
	 *  
	 * @param expression simplified expression 
	 *
	 * @return true when this Neighbor's index contains relevant data
	 */
	public boolean hasAccordingData(String expression) {
		boolean returnValue = true;

		/*if (this.index != null) {
			LocalIndex nbIndex = this.index.getLocalIndexForNeighbor(this);
			// nbIndex=null if a peer logged on and started a query 
			if (nbIndex != null) {
				returnValue = nbIndex.hasAccordingData(expression);
			}
		} else {
			// TODO <KH>: Fehlermeldung auslagern
			System.out.println(
				"!!!!!!!!!!!!!!! Error at Neighbor.hasAccordingData() neighbor index is null --> not yet occured"
			);
		}*/
		return returnValue;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return this.hash;
	}

	/**
	 * method that removes the given updates from the recent updates.
	 * 
	 * @param theARList the list with updates to remove
	 * @param theNeighborID for which neighbor to remove the given updates
	 */
	/*public void removeARListEntriesFromRecentUpdates(Vector<IndexUpdateEntry> theARList,
			String theNeighborID) {
		if (this.index instanceof BucketBasedCompoundRoutingIndex) {
			LocalIndex localIndex = this.index.getLocalIndexForNeighbor(this);
			((BucketBasedLocalIndex) localIndex).removeARListEntriesFromRecentUpdates(theARList, theNeighborID);
		}
	}*/

	/**
	 * method that resets all recents updates and the copy of the index.
	 * 
     * @param theNeighborID for which neighbor to reset the recent updates 
	 */
	/*public void resetRecentUpdates(String theNeighborID) {
		if (this.index instanceof BucketBasedCompoundRoutingIndex) {
			LocalIndex localIndex = this.index.getLocalIndexForNeighbor(this);
			((BucketBasedLocalIndex) localIndex).resetRecentUpdates(theNeighborID);
		}
	}*/

	/**
	 * method for setting this Neighbor object's routing index. 
	 *
	 * @param index CompoundRoutingIndex object that represents the routing index for this Neighbor object.
	 */
	public void setIndex(CompoundRoutingIndex index) {
		this.index = index;
	}

	/**
	 * @param lastTimeUpdatesSent sets lastTimeUpdatesSent
	 */
	public void setLastTimeUpdatesSent(int lastTimeUpdatesSent) {
		this.lastTimeUpdatesSent = lastTimeUpdatesSent;
	}

	/**
	 * @param nextTimeForRequestingUpdates sets nextTimeForRequestingUpdates
	 */
	public void setNextTimeForRequestingUpdates(int nextTimeForRequestingUpdates) {
		this.nextTimeForRequestingUpdates = nextTimeForRequestingUpdates;
	}

	/**
	 * @param querySpaceCache sets querySpaceCache
	 */
	public void setQuerySpaceCache(QuerySpaceCache querySpaceCache) {
		this.querySpaceCache = querySpaceCache;
	}

	
	public String toString() {
		return "Neighbor " + this.neighborID;
	}

	/**
	 * method that adapts the index of this neighbor by inserting data items and remembers update entries. 
	 * 
	 * @param theARList the updates
	 * @param neighborIDsToConsider for which neighbors to create a copy of the index and save the recent updates 
	 */
	/*public void updateIndex(Vector<IndexUpdateEntry> theARList, Vector<String> neighborIDsToConsider) {
		if (this.index instanceof BucketBasedCompoundRoutingIndex) {
			LocalIndex localIndex = this.index.getLocalIndexForNeighbor(this);
			// clone local index if necessary (to compute the changerate later)
			if (neighborIDsToConsider != null) {
				((BucketBasedLocalIndex) localIndex).cloneIndexIfNecessary(neighborIDsToConsider);
			}
			// update the index for the specified neighbor
			try {
				((BucketBasedLocalIndex) localIndex).update(theARList, neighborIDsToConsider);
			} catch (Exception e) {
				System.err.println(
					"could not update the index of peer " + this.localPeer.getPeerID()
					+ " with the data of neighbor " + this.neighborID
				);
				e.printStackTrace();
			}
		}
	}*/
}
