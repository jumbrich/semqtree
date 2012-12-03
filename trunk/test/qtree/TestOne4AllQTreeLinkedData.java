package qtree;
import ie.deri.urq.wods.hashing.CheatingSimpleHashing;
import ie.deri.urq.wods.hashing.QTreeHashing;
import ie.deri.urq.wods.hashing.SimpleHashing;
import ie.deri.urq.wods.hashing.us.MarioHashing;
import ie.deri.urq.wods.hashing.us.PrefixTreeHashing;

import java.io.File;
import java.io.FileInputStream;
import java.net.URLDecoder;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

import junit.framework.TestCase;

import org.semanticweb.wods.indexer.IndexerManager;
import org.semanticweb.wods.indexer.InsertCallback;
import org.semanticweb.wods.indexer.OnDiskQTreeIndexerFactory;
import org.semanticweb.wods.indexer.Queue;
import org.semanticweb.wods.lodq.BGPMatcher;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars2.rdfxml.RDFXMLParser;

import de.ilmenau.datasum.index.OnDiskOne4AllQTreeIndex;

/**
 * 
 */

/**
 * @author hose
 *
 */
public class TestOne4AllQTreeLinkedData extends TestCase {
	
	public static String DATA_DIR = "input/linked-data/";
	private static boolean printTrees = false;
	
	private boolean storeDetailedCounts = false;

	public void testOne4allQTreeLinkedData(QTreeHashing hasher, boolean printQTrees, String hasherName) throws Exception {
		
	    //OnDiskQTreeIndex index = new OnDiskQTreeIndex(hasher, new File("output/"));
	    OnDiskOne4AllQTreeIndex index = new OnDiskOne4AllQTreeIndex(hasher,1500,20,storeDetailedCounts);
	    
	    String dataDir = "input/linked-data/";
	    Queue queue = new Queue(new File(dataDir));
	    IndexerManager manager = new IndexerManager(index, 1, queue, new OnDiskQTreeIndexerFactory(new InsertCallback(index)));
	    manager.runIndexer();
	    
	    
		// test if sourceIDs are inserted correctly
		/*boolean errorFlag = false;
		for (QTreeBucket currBuck: index.getQTree().getAllBuckets()){
			Iterator<String> iter = currBuck.getSourceIDs().iterator();
			while(iter.hasNext()){
				String currSourceID = iter.next();
				QTreeNode currNode = currBuck;
				while (currNode != null){
					//error = error || !currNode.getSourceIDs().contains(currSourceID); // why does this comparison not work? The string is contained here and the containment test returns false???
					boolean contained = false;
					for (String currString : currNode.getSourceIDs()){
						if (currString.equals(currSourceID)){
							contained = true;
							break;
						}
					}
					errorFlag = errorFlag || !contained;
					if (errorFlag) {
						System.err.println("currNode (LID "+currNode.getLocalBucketID()+"): "+currNode.getBoundsString());
						break;
					}
					currNode = currNode.getParent();
				}
				if (errorFlag) {
					System.err.println(", currSourceID: "+currSourceID);
					break;
				}
			}
			if (errorFlag) {
				System.err.println(", currBucket (LID "+currBuck.getLocalBucketID()+"): "+currBuck.getBoundsString());
				break;
			}
		}
		if (errorFlag) {
			//System.err.println(newIndex.getQTree().getStateString());
			//System.exit(0);
			throw new Exception();
		} else System.out.println("passed containment test");*/
	    

	    long qtreeOnDiskSize = index.serialiseQTree(new File("output.qtree4all"));
	    
		long time = System.currentTimeMillis();

		long time1 = System.currentTimeMillis();

		long opti = 0l;
		long stan = 0l;
		
		int queryCnt = 0;
		double globalBenefit = 0.0;
		double globalError = 0.0;
		double globalPrecision = 0.0;
		double globalFallout = 0.0;
		for (Node[] q : org.semanticweb.lodq.SampleBasicQueries.QUERIES) {
			time = System.currentTimeMillis();

			Vector<String> relevantSources = index.getRelevantSourcesForQuery(q);

			time1 = System.currentTimeMillis();
			
			opti += (time1-time);

			time = System.currentTimeMillis();

			File dir = new File(dataDir);
			String[] sources = dir.list();
			
			// determine the actual matches
			BGPMatcher m = new BGPMatcher(q);
			Set<String> results = new HashSet<String>();
			for (String s : sources) {
				String baseurl = URLDecoder.decode(s, "utf-8");
				File f = new File(DATA_DIR + s);
				if (f.isFile()) {
					RDFXMLParser r = new RDFXMLParser(new FileInputStream(DATA_DIR + s), baseurl);
					while (r.hasNext()) {
						Node[] nx = r.next();
						if (m.match(nx)) {
							results.add(baseurl);
							//System.out.println(Nodes.toN3(nx));
						}
					}
				}
			}
			
			time1 = System.currentTimeMillis();
			
			stan += (time1-time);

			// print results
			//int indexMapSize = index.getQTreesMap().entrySet().size();
			int indexMapSize = index.getNmbOfSources();
			System.out.println("Number of sources: "+indexMapSize);
			//			System.out.println(results.size() + " matches, sources " + results);
			System.out.println("\t"+results.size() + " matches");
			double benefit = 1.0-(double)relevantSources.size()/(double)indexMapSize;
			double error = (double)(relevantSources.size()-results.size())/(double)relevantSources.size();
			double precision = (double)(results.size())/(double)relevantSources.size();
			double fallout = (double)(relevantSources.size()-results.size())/(double)(indexMapSize-results.size());
			System.out.println("\tQTree says:"+ relevantSources.size()+" matches, benefit :"+benefit+", error :"+error+", precision: "+precision+", fallout: "+fallout);
			globalBenefit += benefit;
			globalError += error;
			globalPrecision += precision;
			globalFallout += fallout;
			
//			System.out.println(results);
//			System.out.println(relevantSources);
			
			for (String source : relevantSources) {
				if (results.contains(source)) {
					results.remove(source);
				}
			}
			System.out.println("coverage test (should be empty list) " + results);
		}
		System.out.println("=> average benefit:"+(globalBenefit/queryCnt)+", average error :"+(globalError/queryCnt)+", average precision :"+(globalPrecision/queryCnt)+", average fallout :"+(globalFallout/queryCnt));
		
		System.out.println("time elapsed optimised (without query evaluation) " + opti + " ms");
		System.out.println("time elapsed standard query evaluation " + stan + " ms");
		
		
	}
	
	// get hash coordinates using the simple hasher
	public void _testQTreeLinkedDataSimpleHasher() throws Exception {
		System.out.println("======\ntestQTreeLinkedDataSimpleHasher\n======");
		int minCharValue = 32;
		int maxCharValue = 252;
		
		QTreeHashing hasher = new SimpleHashing(minCharValue, maxCharValue); // all ASCII codes between these two 32 = whitespace, 126 = ~
			
		testOne4allQTreeLinkedData(hasher,printTrees,"simple");
	}

	// get hash coordinates using the cheating simple hasher
	public void _testQTreeLinkedDataCheatedSimpleHasher() throws Exception {
		System.out.println("======\ntestQTreeLinkedDataCheatedSimpleHasher\n======");
		int minCharValue = 32;
		int maxCharValue = 252;
		
		int[] minValues = {2,69,-131};
		int[] maxValues = {183,169,183};

		QTreeHashing hasher = new CheatingSimpleHashing(minValues, maxValues, minCharValue, maxCharValue);
			
		testOne4allQTreeLinkedData(hasher,printTrees,"cheat");
	}

	// get hash coordinates using the UniStore prefix tree hasher
	public void testQTreeLinkedDataPrefixTreeHasher() throws Exception {
		System.out.println("======\ntestQTreeLinkedDataPrefixTreeHasher\n======");
		QTreeHashing hasher = new PrefixTreeHashing();
			
		testOne4allQTreeLinkedData(hasher,printTrees,"prefix");
	}

	// get hash coordinates using a very very simple hasher
	public void _testQTreeLinkedDataNaiveHasher() throws Exception {
		System.out.println("======\ntestQTreeLinkedDataNaiveHasher\n======");
		QTreeHashing hasher = new MarioHashing();
			
		testOne4allQTreeLinkedData(hasher,printTrees,"naive");
	}
	
}
