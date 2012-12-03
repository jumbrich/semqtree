package ie.deri.urq.realidy.hashing;

import ie.deri.urq.realidy.bench.utils.Printer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.stats.Count;
import org.semanticweb.yars2.index.disk.QuadStringScanIterator;
import org.semanticweb.yars2.index.disk.block.NodeBlockInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HashDistCallback implements Callback {
	private final static Logger log = LoggerFactory.getLogger(HashDistCallback.class);
	private Count<Integer> hashToS = new Count<Integer>();
	private Count<Integer> hashToP = new Count<Integer>();
	private Count<Integer> hashToO = new Count<Integer>();
	
	int _maxDim = 100000;
	int [] min = {0,0,0};
	int [] max = {_maxDim,_maxDim,_maxDim};

	private File _out;

	private boolean debug;

	private int count =0;
	private OutputStream _outStream;
	private String _hash;
	private QTreeHashing _hashing;


	private File _plotDir;


	private long overAllTime;

	public HashDistCallback(File outDir, File datFile,QTreeHashing hashing, boolean debug) throws FileNotFoundException {
		_maxDim = hashing.getDimSpecMax()[0];
		this.debug = debug;
		_hash = hashing.getHasherName();
		_hashing = hashing;
		_out = outDir;
		_out.mkdirs();
		_outStream = new FileOutputStream(datFile );
	}

	public void endDocument() {
		try{
			Printer.printHashDist(hashToS,1, _hash,_out);
			Printer.printHashDist(hashToP,2,_hash,_out);
			Printer.printHashDist(hashToO,3,_hash,_out);
			if(_outStream!=null)_outStream.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	public long getRealTime(){return overAllTime/1000000;}
	
	public void processStatement(Node[] stmt) {
			long start = System.nanoTime();
			int [] cord = _hashing.getHashCoordinates(Arrays.copyOf(stmt, 3));
			overAllTime+=(System.nanoTime()-start);
			try {
				if(_outStream!=null){
					for (int i =0; i < 2;i++)
						_outStream.write((""+cord[i]+" ").getBytes());
					_outStream.write((""+cord[2]+"\n").getBytes());
				}
			} catch (IOException e) {
			}
			if(debug){
				System.out.println(Nodes.toN3(stmt));
				System.out.println(Arrays.toString(cord));
			}
			put(hashToS,cord[0]);
			put(hashToP,cord[1]);
			put(hashToO,cord[2]);
		count++;
		if(count%10 == 0)log.debug(" Processed {} statements",count);
	}

	private void put(Count<String> c, int v1,
			int v2) {
		c.add(v1+" "+v2);
	}

	private void put(Count<Integer> c,  int value) {
		c.add(value);
	}

	public void startDocument() {
		;
	}
}
