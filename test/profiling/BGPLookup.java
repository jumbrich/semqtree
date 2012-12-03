package profiling;

import ie.deri.urq.wods.hashing.QTreeHashing;
import ie.deri.urq.wods.hashing.us.MarioHashing;

import java.io.File;
import java.io.IOException;
import java.util.Vector;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars2.index.disk.QuadStringScanIterator;
import org.semanticweb.yars2.index.disk.block.NodeBlockInputStream;

import de.ilmenau.datasum.index.OnDiskIndex;
import de.ilmenau.datasum.index.OnDiskOne4AllQTreeIndex;

public class BGPLookup {

    public static void main(String[] args) throws IOException {
	OnDiskOne4AllQTreeIndex index = OnDiskOne4AllQTreeIndex.createFromFile(new File("tmp/qtree4all--hmario_b3_f10_min1000_max1000.ser"));
	
	for (Node[] q : org.semanticweb.lodq.SampleBasicQueries.QUERIES) {
	    Vector<String> relevantSources = index.getRelevantSourcesForQuery(q);
	}
    }
}
