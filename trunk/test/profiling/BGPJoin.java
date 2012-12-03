package profiling;

import ie.deri.urq.wods.hashing.QTreeHashing;
import ie.deri.urq.wods.hashing.us.MarioHashing;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Scanner;
import java.util.Vector;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;
import org.semanticweb.yars2.index.disk.QuadStringScanIterator;
import org.semanticweb.yars2.index.disk.block.NodeBlockInputStream;

import qtree.SampleQueries;

import de.ilmenau.datasum.index.OnDiskIndex;
import de.ilmenau.datasum.index.OnDiskOne4AllQTreeIndex;
import de.ilmenau.datasum.index.QueryResultEstimation;

public class BGPJoin {
    static  boolean run = true;
    public static void main(String[] args) throws IOException, ParseException, InterruptedException {
	OnDiskOne4AllQTreeIndex index = OnDiskOne4AllQTreeIndex.createFromFile(new File("tmp/qtree4all--hmario_b3_f10_min1000_max1000.ser"));
	
//	for (int i = 0; i < SampleQueries.QUERIES.length; i++) {			
//		Node[][] q = SampleQueries.QUERIES[i];
//		int[][] join = SampleQueries.QJ[i];
//		QueryResultEstimation result = index.evaluateQuery2(q, join, false, false);
//		    
//	}
	
	
	Node[][][] QUERIES1 = parseQueries(new File("path"));
	
//	System.out.println("TEST srcMap.containsKey(http://www.sembase.at/index.php/Special:ExportRDF/Daniel_D�gl) = "+srcMap.containsKey("http://www.sembase.at/index.php/Special:ExportRDF/Daniel_D�gl"));
//	System.out.println("TEST srcMap.containsKey(http://www.sembase.at/index.php/Special:ExportRDF/Daniel_D�gl) = "+srcMap.containsKey(URLEncoder.encode("http://www.sembase.at/index.php/Special:ExportRDF/Daniel_D�gl","UTF-8")));
	
	int[][][] QJ = getSubjectObjectJoin(QUERIES1,"path.joinorder");
	
//	for (int i = 0; i < QUERIES1.length; i++) {			
//		Node[][] q = QUERIES1[i];
//		int[][] join = QJ[i];
//		QueryResultEstimation result = index.evaluateQuery2(q, join, false, false);
//		    
//	}
	
	
	index = null;
	final OnDiskOne4AllQTreeIndex index1 = OnDiskOne4AllQTreeIndex.createFromFile(new File("tmp/qtree4all--hprefix_b50000_f5_min0_max50000.ser"));
	final Node[][][] QUERIES2 = parseQueries(new File("tmp/d-2.1.path.joins"));
	
//	System.out.println("TEST srcMap.containsKey(http://www.sembase.at/index.php/Special:ExportRDF/Daniel_D�gl) = "+srcMap.containsKey("http://www.sembase.at/index.php/Special:ExportRDF/Daniel_D�gl"));
//	System.out.println("TEST srcMap.containsKey(http://www.sembase.at/index.php/Special:ExportRDF/Daniel_D�gl) = "+srcMap.containsKey(URLEncoder.encode("http://www.sembase.at/index.php/Special:ExportRDF/Daniel_D�gl","UTF-8")));
	
	final int[][][] QJ2 = getSubjectObjectJoin(QUERIES2,"tmp/d-2.1.path.joins.joinorder");
	
	System.out.println("Benchmarking now!");
	Thread t = new Thread(new Runnable() {
	    
	    public void run() {
		for (int i = 0; i < QUERIES2.length; i++) {			
			Node[][] q = QUERIES2[i];
			int[][] join = QJ2[i];
			QueryResultEstimation result = index1.evaluateQuery2(q, join, false, false);
			System.out.println("[RESULT] "+result.getRelevantSourcesRanked().size());
			run = false;
		}
	    }
	});
	long start = System.currentTimeMillis();
	t.start();
	System.out.println("Thread started!");
	Runtime _runtime = Runtime.getRuntime();
	int mb = 1024*1024;
	long QUERY_TIMEOUT = 600000;
	
	 while(true){
	     long used= _runtime.totalMemory() - _runtime.freeMemory();
	     //Print free memory
	     long free=_runtime.freeMemory();

	     long total = _runtime.totalMemory();

	     //Print Maximum available memory
	     long max = _runtime.maxMemory();
	     System.err.println(new Date(System.currentTimeMillis())+ "[MEM in MB] used:"+used/mb+" max: "+max/mb+" total: "+total/mb+ " free: "+free/mb);
	     if((free/mb) <200){
		 System.out.println("[MEMORY] less then 200 MB free memory -> Garbage Collection");
		 _runtime.gc();_runtime.gc();
	     }
	     try {
		 Thread.sleep(10000);
	     } catch (InterruptedException e) {
		 e.printStackTrace();
	     }
	     if((System.currentTimeMillis()-start) > QUERY_TIMEOUT){
		System.err.println(" Query timeout exceeded -> terminate query");
		run = false;
	    }else if((free/mb) <20){
		System.err.println(" Less then 20 MB free memory -> terminate query");
		run = false;
	    }
	    if(!run){
		t.interrupt();
		t.stop();
		System.out.println("Waiting to terminate thread");
		int counter =0;
		while(t!=null && !t.isAlive()){
		    System.out.println(" ... waiting ....");
		    Thread.sleep(5000);
		    counter++;
		    if(counter == 10){
			try{
			    System.out.println("seeting benchmark to null");
			    t=null;
			}catch(Exception e){
			    e.printStackTrace();
			    System.out.println("Thread still alive: "+t.isAlive());
			    
			}
			
		    }
		}
		System.out.println("benchmark thread terminted");
	    }
	 }
	
    }
    
    private static void parseJoin(Scanner s, List<Node[]> join) throws ParseException {
	String line;
	while(s.hasNextLine()){
	    line = s.nextLine();
	    if(line.startsWith("</JOIN")) return;
	    System.out.println("Adding "+line);
	    join.add(NxParser.parseNodes(line));
	}

    }
    private static Node[][][] parseQueries(File lookupFile) throws FileNotFoundException, ParseException {
	Scanner s = new Scanner(lookupFile);
	String line;
	List<List<Node[]>> joins = new ArrayList<List<Node[]>>();
	while(s.hasNextLine()){
	    line = s.nextLine();
	    List<Node[]> join = new ArrayList<Node[]>();
	    if(line.startsWith("<JOIN>"))
		parseJoin(s,join);
	    joins.add(join);
	    System.out.println(("JOIN: "+join.toString()));
	}

	System.out.println("New array: "+joins.size()+" "+joins.get(0).size());
	Node [][][] res = new Node[joins.size()][joins.get(0).size()][3];

	for(int i =0; i< joins.size(); i++){
	    List<Node[]> join = joins.get(i);
	    for(int a =0; a< join.size(); a++){
		res[i][a] = join.get(a);
	    }
	}

	return res;
    }

    private static int[][][] getSubjectObjectJoin(Node[][][] qUERIES, String line) throws FileNotFoundException {
	int[][][] joinPara = new int [qUERIES.length][qUERIES[0].length-1][2];

	List<int []> idxs = new ArrayList<int[]>();
	Scanner s = new Scanner(new File(line));
	while(s.hasNextLine()){
	    String[] idx= s.nextLine().split(" ");
	    System.out.println(Arrays.toString(idx));
	    int[] idxInt = {Integer.parseInt(idx[0]),Integer.parseInt(idx[1])};
	    idxs.add(idxInt);
	}

	for(int i =0; i<qUERIES.length;i++){
	    for(int a =0; a< qUERIES[i].length-1; a++){
		joinPara[i][a] = idxs.get(a); 
	    }
	}
	return joinPara;
    }
}
