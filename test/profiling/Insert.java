package profiling;

import ie.deri.urq.wods.hashing.QTreeHashing;
import ie.deri.urq.wods.hashing.us.MarioHashing;

import java.io.File;
import java.io.IOException;

import org.semanticweb.yars2.index.disk.QuadStringScanIterator;
import org.semanticweb.yars2.index.disk.block.NodeBlockInputStream;

import de.ilmenau.datasum.index.OnDiskIndex;
import de.ilmenau.datasum.index.OnDiskOne4AllQTreeIndex;

public class Insert {

    public static void main(String[] args) throws IOException {
	QTreeHashing hashing = new MarioHashing();
	OnDiskOne4AllQTreeIndex index = new OnDiskOne4AllQTreeIndex(hashing,3,10,1000,1000,false);
	
	NodeBlockInputStream nbis = new NodeBlockInputStream("input/spoc.idx");
	QuadStringScanIterator iter = new QuadStringScanIterator(nbis);
	
	while(iter.hasNext()){
	    index.addStatmentToIndex(iter.next());
	}
	nbis.close();
	
	index.serialiseQTree(new File("tmp/"));
    }
}
