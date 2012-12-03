package ie.deri.urq.realidy.index;

import de.ilmenau.datasum.index.AbstractIndex;
import de.ilmenau.datasum.index.SingleMultiDimHistogramIndex;
import de.ilmenau.datasum.index.SingleQTreeIndex;
import ie.deri.urq.realidy.hashing.QTreeHashing;

public class SemQTreeFactory {

	private static final String VERSION_QTREE = "qtree";
	private static final String VERSION_HISTO = "histo";

	public static SemQTree create(String indexVersion, QTreeHashing hasher,
			int maxBuckets, int maxFanout, int maxDim,
			boolean storeDetailedCounts) {
		AbstractIndex idx =null;
		if(indexVersion.equalsIgnoreCase(VERSION_QTREE)){
			idx = new SingleQTreeIndex(hasher.getHasherName(), maxBuckets, maxFanout, 0, maxDim, storeDetailedCounts);
		}else if(indexVersion.equalsIgnoreCase(VERSION_HISTO)){
			idx = new SingleMultiDimHistogramIndex(hasher.getHasherName(), maxBuckets,  0, maxDim, storeDetailedCounts);
		}
		 
		SemQTree sqt = new SemQTree(idx);
		return sqt;
	}

}
