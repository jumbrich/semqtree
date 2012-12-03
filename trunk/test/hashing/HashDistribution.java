

import ie.deri.urq.realidy.hashing.HashingFactory;
import ie.deri.urq.realidy.hashing.QTreeHashing;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.stats.Count;
import org.semanticweb.yars2.index.disk.QuadStringScanIterator;
import org.semanticweb.yars2.index.disk.block.NodeBlockInputStream;

public class HashDistribution {
    public static void main(String[] args) throws IOException {
//	for(String hf : HashingFactory.availableHashFunctions()){
    	String hf = "fnvhash";
	    long start = System.currentTimeMillis();
	    File out = new File("tmp.hashes/"+hf+"/");
	    out.mkdirs();
	    
	    NodeBlockInputStream nbis = new NodeBlockInputStream(new File("input/test/1m.nq.idxfile").getAbsolutePath());
	    QuadStringScanIterator iter = new QuadStringScanIterator(nbis);
		
	    Count<Integer> s = new Count<Integer>();
	    Count<Integer> p = new Count<Integer>();
	    Count<Integer> o = new Count<Integer>();
	
	    int maxDim = 100000;
	    ie.deri.urq.realidy.hashing.QTreeHashing hash = (QTreeHashing) HashingFactory.createHasher(hf, new int[]{0,0,0}, new int[]{maxDim,maxDim,maxDim});
	    int [] min = {0,0,0};
	    int [] max = {maxDim,maxDim,maxDim};
	    int count=0;
	    while(iter.hasNext()){
		Node [] n = iter.next();
		
		int [] cord = hash.getHashCoordinates(Arrays.copyOf(n, 3));
		s.add(cord[0]);
		p.add(cord[1]);
		o.add(cord[2]);
		count++;
//		if(count % 100000 == 0)System.out.println(count);
	    }
	    long end = System.currentTimeMillis();
	    System.err.println("["+hf+"] Time elapsed: "+(end-start)+" ms!");
	    
	    PrintStream pw = new PrintStream(new File(out,"s.hash.dist"));
	    print(s,pw);
//	s.printOrderedStats(s.size(), pw);
	    pw.close();
	    pw = new PrintStream(new File(out,"p.hash.dist"));
	    print(p,pw);
//	p.printOrderedStats(s.size(), pw);
	    pw.close();
	    pw = new PrintStream(new File(out,"o.hash.dist"));
	    print(o,pw);
	    pw.close();
	    
//	}
    }

    private static void print(Count<? extends Number> s, PrintStream pw) {
	TreeMap<Number, Integer> sorted = new TreeMap<Number, Integer>();
	sorted.putAll(s);
	for(Entry<Number, Integer> ent: sorted.entrySet()){
	    pw.println(ent.getKey()+" "+ent.getValue());
	}
	
    }
}
