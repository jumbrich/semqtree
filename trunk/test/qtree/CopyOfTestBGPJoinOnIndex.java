package qtree;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Vector;

import junit.framework.TestCase;

import org.semanticweb.hashing.CheatingSimpleHashing;
import org.semanticweb.hashing.QTreeHashing;
import org.semanticweb.hashing.SimpleHashing;
import org.semanticweb.hashing.us.MarioHashing;
import org.semanticweb.hashing.us.PrefixTreeHashing;
import org.semanticweb.indexer.IndexerManager;
import org.semanticweb.indexer.InsertCallback;
import org.semanticweb.indexer.OnDiskQTreeIndexerFactory;
import org.semanticweb.indexer.Queue;
import org.semanticweb.lodq.BGPMatcher;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.parser.ParseException;
import org.semanticweb.yars2.index.disk.QuadStringScanIterator;
import org.semanticweb.yars2.index.disk.block.NodeBlockInputStream;
import org.semanticweb.yars2.rdfxml.RDFXMLParser;

import com.sleepycat.collections.MapEntryParameter;

import de.ilmenau.qtree.index.Bucket;
import de.ilmenau.qtree.index.IntersectionInformation;
import de.ilmenau.qtree.index.OnDiskOne4AllQTreeIndex;
import de.ilmenau.qtree.index.QueryResultEstimation;
import de.ilmenau.qtree.index.QuerySpace;
import de.ilmenau.qtree.index.qtree.QTree;
import de.ilmenau.qtree.index.qtree.QTreeBucket;
import de.ilmenau.qtree.index.update.UpdateBucket;
import de.ilmenau.qtree.util.bigmath.Space;

public class CopyOfTestBGPJoinOnIndex extends TestCase {
    public static String DATA_DIR = "input/linked-data/";
    private int DEBUG = 1;

    int dimMin = 0;
    int dimMax = 10000; // upper limit...!!

    boolean storeDetailedCounts = true;
    boolean advancedRanking = false;

    public void testBGP() throws Exception {
	long time = System.currentTimeMillis();
	//		System.setProperty("file.encoding","utf-8");
	//		System.err.println(System.getProperty("file.encoding"));
	File dir = new File(DATA_DIR);
	String[] sources = dir.list();

//			QueryResultEstimation result = index.evaluateQuery(q, join, true, false);
			QueryResultEstimation result = index.evaluateQuery2(q, join, false, false);
			try{
				// do the actual BGP processing
				Map<Node[],String> current = evaluateBgp(sources, q[0]);
				System.out.println("Qtree says: "+result.getBgpEstSources()[0]+" relevant sources");

	File outputDir = new File("output");
	File inputIDX = new File("input/spoc.idx");
	OnDiskOne4AllQTreeIndex index = new OnDiskOne4AllQTreeIndex(hasher,1500,20,new int[]{dimMin,dimMin,dimMin}, new int[] {dimMax,dimMax,dimMax},storeDetailedCounts);
	buildOrLoad(outputDir, index, inputIDX);
	//	    String dataDir = "input/linked-data/";
	//	    Queue queue = new Queue(new File(dataDir));
	//	    IndexerManager manager = new IndexerManager(index, 1, queue, new OnDiskQTreeIndexerFactory());
	//	    manager.runIndexer();

	System.out.println("QTree: "+index.getQTree().getAllBuckets().size()+" buckets");
	//		System.out.println(index.getQTree().getStateString());
	//		System.exit(1);
	//		index.getQTree().printme();

<<<<<<< .mine
	for (int i = 0; i < SampleQueries.QUERIES.length; i++) {			
	    Node[][] q = SampleQueries.QUERIES[i];
	    int[][] join = SampleQueries.QJ[i];
	    //		for (int i = 0; i < SampleQueries.QUERIESSLOW.length; i++) {			
	    //			Node[][] q = SampleQueries.QUERIESSLOW[i];
	    //			int[][] join = SampleQueries.QJS[i];


	    QueryResultEstimation result = index.evaluateQuery(q, join, true, false);

	    File f = new File("q-"+i+".ser");
	    FileOutputStream fos;
	    try {
		fos = new FileOutputStream(f);
		ObjectOutputStream oos = new ObjectOutputStream(fos);
		oos.writeObject(result);
		oos.close();
		fos.close();

	    } catch (Exception e) {
		e.printStackTrace();

	    }

	    try{
		// do the actual BGP processing
		Map<Node[],String> current = evaluateBgp(sources, q[0]);
		System.out.println("Qtree says: "+result.getBgpEstSources()[0]+" relevant sources");

		for (int j = 1; j < q.length; j++) {
		    // do the actual BGP processing
		    Map<Node[],String> gpresults = evaluateBgp(sources, q[j]);		
		    System.out.println("Qtree says: "+result.getBgpEstSources()[j]+" relevant sources");

		    System.out.println("joining "+current.size()+" triples with "+gpresults.size());
		    current = computeJoin(current, join[j-1][0], gpresults, join[j-1][1]);
		    System.out.println("result: "+current.size()+" triples");
=======
				if (0 == current.size()) System.out.println("Empty join result.");
				Set<String> resSources = new HashSet<String>();
				for (Map.Entry<Node[],String> nx : current.entrySet()) {
					//				for (Node n : nx.getKey()) System.out.print(n.toN3()+" ");
					//				System.out.println();
					resSources.add(nx.getValue());
				}
				Set<String> allResSources = new HashSet<String>();
				for (String s : resSources) {
					StringTokenizer tok = new StringTokenizer(s,"|");
					while (tok.hasMoreTokens()) allResSources.add(URLDecoder.decode(tok.nextToken(), "utf-8"));
				}
				System.out.println("Actual "+allResSources.size()+" relevant sources:");
//				System.out.println("Actual "+resSources.size()+" relevant sources:");
				//			for (String s : resSources) {
				//				System.out.println(s);
				////				System.out.println(s+" -- "+(currRelevantSources.containsKey(URLDecoder.decode(s, "utf-8"))? "ok" : "missing!"));
				//			}
				
				ArrayList<String> rankOrderedSources = advancedRanking? result.getRelevantSourcesRankedAdvanced() : result.getRelevantSourcesRanked();
//				System.out.println(rankOrderedSources);
				// the k values for evaluating top-k
				int[] topK = new int[]{10,50,100};
				// to store the percentages of each top-k result set
				double[] topKPerc = new double[topK.length];
				// handle each of the k values
				for (int k=0; k<topK.length; ++k) {
//					topKPerc[k] = computeTopKPercentage(ranked, topK[k], current);
					topKPerc[k] = computeTopKPercentage(rankOrderedSources, topK[k], current);
				}
				
				// compare the QTree ranks with the actual ranks
				Vector<Integer> positions = new Vector<Integer>();
				double avgErr = 0.0;
//				avgErr = getRankError(ranked, current, positions);
				avgErr = getRankError(rankOrderedSources, current, positions);
				
				// check at which k we will achieve 100% of the result (i.e., contain all contributing sources)
//				int maxK = getMaxK(ranked, allResSources, positions);
				
//				int maxK = advancedRanking? positions.lastElement() : positions.firstElement();
				int maxK = positions.lastElement();
				
				System.out.println("all actually relevant sources in QTree's top-"+maxK+" (avg error: "+avgErr+", positions: "+positions+")");
				for (int k=0; k<topK.length; ++k) {
					System.out.println("\ttop-"+topK[k]+" contains: "+topKPerc[k]+" of result");
				}
				System.out.println("times:");
				System.out.print("\tBGPs: ");
				for (long t : result.getBgpEvalTimes()) System.out.print(t+" ");
				System.out.println();
				System.out.print("\tjoins: ");
				for (long t : result.getJoinEvalTimes()) System.out.print(t+" ");
				System.out.println();
				System.out.println("\tranking: "+(advancedRanking? result.getAdvancedRankTime(): result.getRankTime()));
			}catch(Exception e){
				e.printStackTrace();
			}
			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
>>>>>>> .r2359
		}

		System.out.println("relevant "+result.getRelevantSourcesRanked().size()+" sources according to QTree:");
		//			for (String s : relevantSources) {
		//				System.out.println(s);
		//			}

		if (0 == current.size()) System.out.println("Empty join result.");
		Set<String> resSources = new HashSet<String>();
		for (Map.Entry<Node[],String> nx : current.entrySet()) {
		    //				for (Node n : nx.getKey()) System.out.print(n.toN3()+" ");
		    //				System.out.println();
		    resSources.add(nx.getValue());
		}
		Set<String> allResSources = new HashSet<String>();
		for (String s : resSources) {
		    StringTokenizer tok = new StringTokenizer(s,"|");
		    while (tok.hasMoreTokens()) allResSources.add(URLDecoder.decode(tok.nextToken(), "utf-8"));
		}
		System.out.println("Actual "+allResSources.size()+" relevant sources:");
		//				System.out.println("Actual "+resSources.size()+" relevant sources:");
		//			for (String s : resSources) {
		//				System.out.println(s);
		////				System.out.println(s+" -- "+(currRelevantSources.containsKey(URLDecoder.decode(s, "utf-8"))? "ok" : "missing!"));
		//			}

		ArrayList<String> rankOrderedSources = advancedRanking? result.getRelevantSourcesRankedAdvanced() : result.getRelevantSourcesRanked();
		// the k values for evaluating top-k
		int[] topK = new int[]{10,50,100};
		// to store the percentages of each top-k result set
		double[] topKPerc = new double[topK.length];
		// handle each of the k values
		for (int k=0; k<topK.length; ++k) {
		    //					topKPerc[k] = computeTopKPercentage(ranked, topK[k], current);
		    topKPerc[k] = computeTopKPercentage(rankOrderedSources, topK[k], current);
		}

		// compare the QTree ranks with the actual ranks
		Vector<Integer> positions = new Vector<Integer>();
		double avgErr = 0.0;
		//				avgErr = getRankError(ranked, current, positions);
		avgErr = getRankError(rankOrderedSources, current, positions);

		// check at which k we will achieve 100% of the result (i.e., contain all contributing sources)
		//				int maxK = getMaxK(ranked, allResSources, positions);

		//				int maxK = advancedRanking? positions.lastElement() : positions.firstElement();
		int maxK = positions.lastElement();

		System.out.println("all actually relevant sources in QTree's top-"+maxK+" (avg error: "+avgErr+", positions: "+positions+")");
		for (int k=0; k<topK.length; ++k) {
		    System.out.println("\ttop-"+topK[k]+" contains: "+topKPerc[k]+" of result");
		}
		System.out.println("times:");
		System.out.print("\tBGPs: ");
		for (long t : result.getBgpEvalTimes()) System.out.print(t+" ");
		System.out.println();
		System.out.print("\tjoins: ");
		for (long t : result.getJoinEvalTimes()) System.out.print(t+" ");
		System.out.println();
		System.out.println("\tranking: "+(advancedRanking? result.getAdvancedRankTime(): result.getRankTime()));
		
		
		
		
	    }catch(Exception e){
		e.printStackTrace();
	    }
	    System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
	}

    }

    private Double computeTopKPercentage(TreeMap<Double,String> ranked, int k, Map<Node[],String> resTriples) throws Exception {
	// this would speed up the calculation - currently, we can also check if the whole set really contains all actually relevant sources
	//			if (topK[k]<=ranked.size()) topKPerc[k] = 1.0;
	// determine the top-kth key
	double fromKey = ranked.firstKey();
	// if not, the top-kth key is the lowest (i.e., the first) in the ranked set
	if (k<=ranked.size()) {
	    NavigableSet<Double> keys = ranked.descendingKeySet();
	    //				for (int z=0; z<topK[k]-1; ++z) keys.pollFirst();
	    //				fromKey = keys.pollFirst();
	    // iterate over all keys descending from the end of the set
	    Iterator<Double> it = keys.iterator();
	    // skip the k-1 top keys
	    for (int z=0; z<k-1; ++z) it.next();
	    // that's the top-kth key now
	    fromKey = it.next();
	}
	//			System.out.println("tail-"+topK[k]+": "+ranked.tailMap(fromKey).size()+": "+ranked.tailMap(fromKey));
	int resTriple = 0;
	// iterate over all result triples 
	for (Map.Entry<Node[],String> nx : resTriples.entrySet()) {
	    // extract the single sources contributing to the result triple
	    StringTokenizer tok = new StringTokenizer(nx.getValue(),"|");
	    boolean allSourcesIncl = true;
	    // check if all contributing sources are in the top-k sources set
	    while (tok.hasMoreTokens() && allSourcesIncl) {
		if (!ranked.tailMap(fromKey).values().contains(URLDecoder.decode(tok.nextToken(), "utf-8"))) allSourcesIncl = false;
	    }
	    // are all contributing sources included in the top-k sources set?
	    if (allSourcesIncl) ++resTriple;
	}
	// determine the actual percentage we could get when only using the top-k sources
	return (double)resTriple/(double)resTriples.size();
    }

    private Double computeTopKPercentage(ArrayList<String> ranked, int k, Map<Node[],String> resTriples) throws Exception {
	// this would speed up the calculation - currently, we can also check if the whole set really contains all actually relevant sources
	//			if (topK[k]<=ranked.size()) topKPerc[k] = 1.0;
	int resTriple = 0;
	// iterate over all result triples 
	for (Map.Entry<Node[],String> nx : resTriples.entrySet()) {
	    // extract the single sources contributing to the result triple
	    StringTokenizer tok = new StringTokenizer(nx.getValue(),"|");
	    boolean allSourcesIncl = true;
	    // check if all contributing sources are in the top-k sources set
	    while (tok.hasMoreTokens() && allSourcesIncl) {
		String srcToken =tok.nextToken();
		int pos = ranked.indexOf((URLDecoder.decode(srcToken, "utf-8")));
		
//		if(-1 != pos && k > pos)
//		     System.err.println("SRC:"+URLDecoder.decode(srcToken, "utf-8")+ " is in top-"+k+" rank: "+pos);
		if (-1 == pos || k <= pos) allSourcesIncl = false;
	    }
	    // are all contributing sources included in the top-k sources set?
	    if (allSourcesIncl) ++resTriple;
	}
	System.out.println("I have resTriples "+resTriple+" out of "+resTriples.size()+" for top-k "+k);
	System.out.println((double)resTriple/(double)resTriples.size());// determine the actual percentage we could get when only using the top-k sources
	
	
	// determine the actual percentage we could get when only using the top-k sources
	return (double)resTriple/(double)resTriples.size();
    }

    private Map<String,Integer> getSourceRanks(Map<Node[],String> resultTriples) throws Exception {
	Map<String,Double> ranks = new HashMap<String,Double>();
	int tokCnt = 0;
	for (Map.Entry<Node[],String> nx : resultTriples.entrySet()) {
	    StringTokenizer tok = new StringTokenizer(nx.getValue(),"|");
	    while (tok.hasMoreTokens()) {
		// we only have to count once
		if (0 == tokCnt) ++tokCnt;
		String source = URLDecoder.decode(tok.nextToken(), "utf-8");
		Double cnt = ranks.get(source);
		if (null == cnt) cnt = 0.0;
		cnt += 1.0;
		ranks.put(source,cnt);
	    }
	}
	TreeMap<Double,String> ranked = new TreeMap<Double, String>();
	for (Map.Entry<String,Double> sources : ranks.entrySet()) {
	    String sourceName = sources.getKey();
	    double cnt = sources.getValue();
	    // this is one approach: 30 result triples, 3 sources -> assign 10 to each source...
	    //			cnt /= (double)tokCnt;
	    // ...actually, the cummulated one works better: 30 result triples, 3 sources -> assign 30 to each source (i.e., sum(assignment)=#restriples*#joinlevel)
	    while (ranked.containsKey(cnt)) cnt += 0.000001;
	    ranked.put(cnt,sourceName);
	}
	Map<String,Integer> sourceRanks = new HashMap<String,Integer>();
	int cnt = 0;
	for (String s : ranked.values()) {
	    sourceRanks.put(s,ranked.size()-cnt);
	    ++cnt;
	}
	//		System.out.println(ranked);
	return sourceRanks;
    }

    private double getRankError(TreeMap<Double,String> ranked, Map<Node[],String> resTriples, Vector<Integer> positions) throws Exception {
	Map<String,Integer> actRanks = getSourceRanks(resTriples);
	double avgErr = 0.0;
	int cnt = 0;
	int notFound = 0;
	System.out.print("rank errors: ");
	for (String s : ranked.values()) {
	    if (advancedRanking) s = s.substring(s.indexOf('|')+1);
	    if (actRanks.containsKey(s)) {
		int qtreeRank = ranked.size()-cnt;
		positions.add(qtreeRank);
		//				System.out.println("QTree rank="+qtreeRank+"; actual rank="+actRanks.get(s)+"; error="+Math.abs(qtreeRank-actRanks.get(s)));
		System.out.print("|"+qtreeRank+"-"+actRanks.get(s)+"|; ");
		avgErr += Math.abs(qtreeRank-actRanks.get(s));
	    }
	    else ++notFound;
	    ++cnt;
	}
	System.out.println();
	// average over only the actually contained sources
	avgErr /= (double)(ranked.size()-notFound);
	return avgErr;
    }

    private double getRankError(ArrayList<String> ranked, Map<Node[],String> resTriples, Vector<Integer> positions) throws Exception {
	Map<String,Integer> actRanks = getSourceRanks(resTriples);
	double avgErr = 0.0;
	int cnt = 0;
	int notFound = 0;
	System.out.print("rank errors: ");
	for (String s : ranked) {
	    if (actRanks.containsKey(s)) {
		int qtreeRank = cnt+1;
		positions.add(qtreeRank);
		//				System.out.println("QTree rank="+qtreeRank+"; actual rank="+actRanks.get(s)+"; error="+Math.abs(qtreeRank-actRanks.get(s)));
		System.out.print("|"+qtreeRank+"-"+actRanks.get(s)+"|; ");
		avgErr += Math.abs(qtreeRank-actRanks.get(s));
	    }
	    else ++notFound;
	    ++cnt;
	}
	System.out.println();
	// average over only the actually contained sources
	avgErr /= (double)(ranked.size()-notFound);
	return avgErr;
    }

    public Map<Node[],String> computeJoin(Map<Node[],String> l, int lpos, Map<Node[],String> r, int rpos) {
	Map<Node[],String> result = new HashMap<Node[],String>();

	for (Map.Entry<Node[],String> lnx : l.entrySet()) {
	    Node ljc = lnx.getKey()[lpos];
	    for (Map.Entry<Node[],String> rnx : r.entrySet()) {
		Node rjc = rnx.getKey()[rpos];

		if (ljc.equals(rjc)) {
		    Node[] comb = new Node[lnx.getKey().length+rnx.getKey().length];
		    System.arraycopy(lnx.getKey(), 0, comb, 0, lnx.getKey().length);
		    System.arraycopy(rnx.getKey(), 0, comb, lnx.getKey().length, rnx.getKey().length);

		    //					result.put(comb,"l:"+lnx.getValue()+"|r:"+rnx.getValue());
		    result.put(comb,lnx.getValue()+"|"+rnx.getValue());
		}
	    }
	}

	return result;
    }

    public Map<Node[],String> evaluateBgp(String[] sources, Node[] bgp) throws FileNotFoundException, ParseException, IOException {
	BGPMatcher m = new BGPMatcher(bgp);

	System.out.println("Query for " + Nodes.toN3(bgp));

	Map<Node[],String> results = new HashMap<Node[],String>();

	int triplesCount = 0;
	for (String s : sources) {
	    String baseurl = URLDecoder.decode(s, "utf-8");

	    File f = new File(DATA_DIR + s);
	    if (f.isFile()) {
		RDFXMLParser r = new RDFXMLParser(new FileInputStream(DATA_DIR + s), baseurl);

		while (r.hasNext()) {
		    Node[] nx = r.next();
		    triplesCount++;
		    if (m.match(nx)) {
			results.put(nx,s);
			//						System.out.println(s+": "+Nodes.toN3(nx));
		    }
		}
	    }
	}
	System.err.println("{TTT] parsed "+triplesCount+" triples");

	return results;
    }

    private void buildOrLoad(File outputDir, OnDiskOne4AllQTreeIndex index, File inputIDX) throws IOException {
	outputDir.mkdirs();	   
	File qtreeFile = new File(outputDir,index.getFileName());
	if(qtreeFile.exists()){
	    System.err.println("[DEBUG] Found serialised version of a qtree in the output dir "+outputDir);
	    index.loadQTreeIndex(outputDir);
	    System.out.println(index.getLabel());
	    System.out.println(index.getQTree().getRoot().getCount());
	    System.err.println(" <---------------->\n");
	    //	    System.err.println("  stmts: "+insertCnt);
	    System.err.println("  srcs: "+index.getNmbOfSources());
	    System.err.println("  index: ["+index.getLabel()+" pts: "+index.getQTree().getRoot().getCount()+" src: "+index.getNmbOfSources()+" ]");
	    System.out.println("  Buckets: "+index.getQTree().getAllBuckets().size());
	    System.out.println("  Dimension: "+index.getQTree().getNmbOfDimensions());
	    System.out.println("  IndexStatments: "+index.getQTree().getNmbOfIndexedItems());
	    //	    System.err.println("  insert/ms, "+(((double) insertCnt/(double)(end-time))));


	}
	else{
	    System.err.println("[DEBUG] Could not found serialised version of a qtree in the output dir "+outputDir);
	    System.err.println("[DEBUG] Building new QTree from statements in "+ inputIDX);

	    InsertCallback c = new InsertCallback(index);
	    NodeBlockInputStream nbis = new NodeBlockInputStream(inputIDX.getAbsolutePath());
	    QuadStringScanIterator iter = new QuadStringScanIterator(nbis);
	    int count=0;
	    while(iter.hasNext()){
		//System.out.print(".");
		c.processStatement(iter.next());
		count++;
		if(count%10000==0)
		    System.err.println("DEBUG inserted "+count+" stmts");
	    }
	    System.out.println("inserted "+c.insertedStatments()+" stmts");
	    System.err.println("[DEBUG] Serialising QTree from statements to "+ outputDir);
	    System.out.println("Buckets: "+index.getQTree().getAllBuckets().size());
	    System.out.println("Dimension: "+index.getQTree().getNmbOfDimensions());
	    System.out.println("IndexStatments: "+index.getQTree().getNmbOfIndexedItems());
	    index.serialiseQTree(outputDir);
	}
    }
}
