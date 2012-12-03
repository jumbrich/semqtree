/*
 *
 */
package de.ilmenau.datasum.index.histogram;

import java.util.Vector;

import de.ilmenau.datasum.index.BucketBasedCompoundRoutingIndex;
import de.ilmenau.datasum.index.Index;
import de.ilmenau.datasum.index.LocalIndex;
import de.ilmenau.datasum.peer.Neighbor;

/**
 * This class represents a compound routing index by using multidimensional histograms.
 * 
 * @author Christian Lemke
 * @author Katja Hose
 * @version $Id: MultiDimRoutingHistogram.java,v 1.6 2007-05-21 13:33:44 lemke Exp $
 */
@SuppressWarnings("serial")
public class MultiDimRoutingHistogram extends BucketBasedCompoundRoutingIndex {

	/** the LocalIndex for this routing index */
	private MultiDimHistogram correspondingIndex = null;


	/**
	 * creates an "empty/all-purpose" routing index 
	 */
	public MultiDimRoutingHistogram() {
		super(IndexType.MultiDimHistogram);
	}

	/**
	 * constructor that uses the input MultiDimHistogram as basis for its compoundRoutingIndex
	 * 
	 * @param inputIndex
	 */
	public MultiDimRoutingHistogram(MultiDimHistogram inputIndex) {
		super(IndexType.MultiDimHistogram);
		this.correspondingIndex = (MultiDimHistogram) inputIndex.clone();

	}

	/**
	 * creates a new routing histogram out of a number of MultiDimHistograms
	 * 
	 * @param indexesToMerge 
	 * @param neighbor 
	 */
	/*public MultiDimRoutingHistogram(Vector<Index> indexesToMerge, Neighbor neighbor) {
		super(IndexType.MultiDimHistogram);
		MultiDimHistogram emptyHistogram = (MultiDimHistogram) neighbor.createEmptyLocalIndex();

		for (Index currIndex : indexesToMerge) {
			assert (currIndex instanceof MultiDimHistogram);
			MultiDimHistogram currHistogram = (MultiDimHistogram) currIndex;

			assert (MultiDimHistogram.compareDefinitions(currHistogram, emptyHistogram));
			emptyHistogram.addIndexData(currHistogram);

		}
		this.correspondingIndex = emptyHistogram;
		// be sure that the parent name is set!
		if (this.correspondingIndex.getParentNodeName() == null) {
			this.correspondingIndex.setParentNodeName(
				((MultiDimHistogram) neighbor.getCorrespondingPeerObject().getLRI()).getParentNodeName()
			);
		}
	}*/

	/* (non-Javadoc)
	 * @see smurfpdms.index.CompoundRoutingIndex#addIndexData(smurfpdms.peer.Neighbor, smurfpdms.index.LocalIndex)
	 */
	//@Override
	/*public void addIndexData(Neighbor neighbor, LocalIndex inputIndex) {
		assert (inputIndex instanceof MultiDimHistogram);
		if (this.correspondingIndex == null) {
			this.correspondingIndex = (MultiDimHistogram) neighbor.createEmptyLocalIndex();
		}
		this.correspondingIndex.addIndexData((MultiDimHistogram) inputIndex);
	}*/

	/* (non-Javadoc)
	 * @see smurfpdms.index.Index#clone()
	 */
	@Override
	public Object clone() {
		MultiDimRoutingHistogram clone = (MultiDimRoutingHistogram) super.clone();
		if (this.correspondingIndex != null) {
			clone.correspondingIndex = (MultiDimHistogram) this.correspondingIndex.clone();
		}
		return clone;
	}

	/* (non-Javadoc)
	 * @see smurfpdms.index.CompoundRoutingIndex#getLocalIndexForNeighbor(smurfpdms.peer.Neighbor)
	 */
	//@Override
	public LocalIndex getLocalIndexForNeighbor(Neighbor neighbor) {
		return this.correspondingIndex;
	}

	/* (non-Javadoc)
	 * @see smurfpdms.index.Index#getStateString()
	 */
	@SuppressWarnings("nls")
	@Override
	public String getStateString() {
		if (this.correspondingIndex == null) {
			return "--> empty routing index: MultiDimRoutingHistogram";
		}
		return this.correspondingIndex.getStateString();
	}

	/* (non-Javadoc)
	 * @see smurfpdms.index.Index#isEmpty()
	 */
	@Override
	public boolean isEmpty() {
		if (this.correspondingIndex == null) {
			return true;
		}
		return this.correspondingIndex.isEmpty();
	}
}
