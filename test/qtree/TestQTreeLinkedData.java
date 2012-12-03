package qtree;
/**
 * 
 */
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

import org.semanticweb.wods.indexer.Queue;
import org.semanticweb.wods.lodq.BGPMatcher;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars2.rdfxml.RDFXMLParser;

import de.ilmenau.datasum.index.OnDiskQTreeIndex;

/**
 * @author hose
 *
 */
public class TestQTreeLinkedData extends TestCase {
	
	boolean storeDetailedCounts = false;
	
	public void testQTreeLinkedData(QTreeHashing hasher, boolean printQTrees, String hasherName) throws Exception {
		
	    OnDiskQTreeIndex index = new OnDiskQTreeIndex(hasher ,new File("output/"),storeDetailedCounts);
	    
	    String dataDir = "input/linked-data/";
	    Queue queue = new Queue(new File(dataDir));
	    //IndexerManager manager = new IndexerManager(index, 2, queue,new OnDiskQTreeIndexerFactory());
	    //manager.runIndexer();
	    
//	    if(printQTrees){
//		for(Entry<String, QTree> entry: index.getQTreesMap().entrySet()){
//		    String s = URLEncoder.encode(entry.getKey(),"UTF-8");
//		     printQTreeAsGNUPlot(entry.getValue(), s+"--"+hasherName, "output/", data, true, true, 1, 2);
//		     SemanticQTreeTest.printQTreeAsGNUPlot(currQTree, s+"--"+hasherName, "output/", data, false, true);
//		}
//	    }
	    
//	    int dimensions = 3;
//		int fanout = 5;
//		int buckets = 30;
//		
//		HashMap<String, QTree> sourcesQTrees = new HashMap<String, QTree>();
//			
//		int[] dimSpecMin = {0,0,0}; //int[] minValues = {0,0,0};
//		int[] dimSpecMax = {1000,1000,1000}; //int[] maxValues = {1,1,875};
//		
//		// remove longest common prefixes and suffixes -- the data is so different, there are no commen prefixes and suffixes
//		//StringHelper.trimmingCommonPrefixesAndSuffixes(stringData);
//				
//		/*if (cheating){
//			// read data
//			Vector<String[]> stringData = readLinkedData();
//		
//			//compute minimum and maximum hash values for all dimensions -- and set the parameters in the CheatingSimpleHashing class
//			CheatingSimpleHashing cheatHasher = new CheatingSimpleHashing(minCharValue, maxCharValue); 
//			Vector<int[]> minMaxValues = cheatHasher.getMinMaxHashValuesForAllDimensions(stringData, dimSpecMin, dimSpecMax);
//			minValues = minMaxValues.firstElement();
//			maxValues = minMaxValues.elementAt(1);
//			System.out.println("min: "+Arrays.toString(minValues)+" --- "+"max: "+Arrays.toString(maxValues));
//			System.exit(0);
//		}*/
//
		long time = System.currentTimeMillis();
//		
//		// start to read data
//		String dataDir = "input/linked-data/";
//		
//		File dir = new File(dataDir);
//		String[] sources = dir.list();
//
//		long dataSize = 0;
//		long treeSize = 0;
//		for (String s : sources) {
//			String baseurl = URLDecoder.decode(s, "utf-8");
//
//			File f = new File(dataDir + s);
//			if (f.isFile()) {
//				dataSize += f.length();
//				// collect input tuples
//				Vector<String[]> tuples = new Vector<String[]>();
//
//				// read file
//				RDFXMLParser r = new RDFXMLParser(new FileInputStream(dataDir + s), baseurl);
//				while (r.hasNext()) {
//					Node[] nx = r.next();
//					//System.out.println(Arrays.toString(nx));
//
//					String[] converted = {nx[0].toN3(), nx[1].toN3(), nx[2].toN3()};
//					tuples.add(converted);
//				}
//
//				// collect int coordinate data (for printing the QTree into a file)
//				int[][] data = null;
//				if (printQTrees) data = new int[tuples.size()][dimensions];
//
//				// initiate QTree
//				QTree currQTree = new QTree(dimensions, fanout, buckets, dimSpecMin, dimSpecMax, "0", "1");
//
//				// insert data into QTree
//				for (int i=0;i<tuples.size();i++){
//
//					String[] item = tuples.elementAt(i);
//
//					int[] hashCoordinates = hasher.getHashCoordinates(item, dimSpecMin, dimSpecMax);
//
//					if (printQTrees) data[i] = hashCoordinates;
//					//System.out.println(Arrays.toString(hashCoordinates));
//
//					// insert into QTree
//					try {
//						currQTree.insertDataItem(hashCoordinates);
//					} catch (Exception e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//				}
//
//				sourcesQTrees.put(s,currQTree);
//
//				// output current QTree state as a gnuplot diagram
////				if (printQTrees) printQTreeAsGNUPlot(currQTree, s+"--"+hasherName, "output/", data, true, true, 1, 2);
//				if (printQTrees) SemanticQTreeTest.printQTreeAsGNUPlot(currQTree, s+"--"+hasherName, "output/", data, false, true);
//
//				String objFileName = "output/"+s+"--"+hasherName+".obj";
//				ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(objFileName));
//				out.writeObject(currQTree);
//				out.close();
//				File treeFile = new File(objFileName);
//				treeSize += treeFile.length();
//			}
//		}
//		
		long time1 = System.currentTimeMillis();
//
//		System.out.println("alle QTrees aufgebaut in " + (time1 - time) + " ms, size in bytes on disk: "+treeSize+", input data size in bytes on disk: "+dataSize);

		// TODO: und jetzt der eigentliche Test 
		//sourcesQTrees

		// should be 1 source:
//		String[] query = {"http://www.w3.org/2000/01/rdf-schema#", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.w3.org/2002/07/owl#Ontology"};
		// should be 2 sources:
//		String[] query = {"http://harth.org/andreas/foaf#ah", "http://xmlns.com/foaf/0.1/knows", "http://compsoc.nuigalway.ie/~fuzz/foaf.rdf#Fergal_Monaghan"};
		// should be 3 sources:
//		String[] query = {"?", "http://xmlns.com/foaf/0.1/knows", "http://compsoc.nuigalway.ie/~fuzz/foaf.rdf#Fergal_Monaghan"};

//		int[] hashCoordinates = hasher.getHashCoordinates(query, dimSpecMin, dimSpecMax);

		// get the query space
		// wenn eine Dimension nicht spezifiert ist, dann einfach den gesamten Bereich nehmen, siehe oben dimSpecMin und dimSpecMax (Position entspricht Dimension)
		// Beispielanfrage auf einen Punkt mit den QTree-Koordinaten: [?,200,800], d.h. die erste Dimension ist egal deshalb als Angabe der volle Bereich von 0 bis 1000
		//int[] lowerBoundaries = {0,200,800};
		//int[] upperBoundaries = {1000,200,800};
		
//		int[] lowerBoundaries = Arrays.copyOf(hashCoordinates,hashCoordinates.length);
//		int[] upperBoundaries = Arrays.copyOf(hashCoordinates,hashCoordinates.length);

		long opti = 0l;
		long stan = 0l;
		
		int queryCnt = 0;
		double globalBenefit = 0.0;
		double globalError = 0.0;
		double globalPrecision = 0.0;
		double globalFallout = 0.0;
		for (Node[] q : org.semanticweb.lodq.SampleBasicQueries.QUERIES) {
			time = System.currentTimeMillis();

//			++queryCnt;
//			int[] hashCoordinates = hasher.getHashCoordinates(q, dimSpecMin, dimSpecMax);
//			int[] lowerBoundaries = Arrays.copyOf(hashCoordinates,hashCoordinates.length);
//			int[] upperBoundaries = Arrays.copyOf(hashCoordinates,hashCoordinates.length);
//			for (int i=0;i<q.length;i++) {
//				if (q[i] instanceof Variable) {
//					// variablen
//					lowerBoundaries[i] = dimSpecMin[i];
//					upperBoundaries[i] = dimSpecMax[i];
//				}
//			}
//
//			System.out.println("query"+queryCnt+":" + Nodes.toN3(q));
//
////			System.err.print("\tlower: ");
////			for (int i : lowerBoundaries) { System.err.print(i+" "); }
////			System.err.print("; upper: ");
////			for (int i : upperBoundaries) { System.err.print(i+" "); }
////			System.err.println();

			Vector<String> relevantSources = index.getRelevantSourcesForQuery(q);
//			QuerySpace querySpace = new QuerySpace(lowerBoundaries, upperBoundaries, false);
//
//			Vector<String> relevantSources = new Vector<String>();
//
//			for (Map.Entry<String,QTree> currEntry: sourcesQTrees.entrySet()){
//				QTree testTree = currEntry.getValue();
//
//				BucketBasedLocalIndex bucketIndex = (BucketBasedLocalIndex) testTree;
//
//				// get the buckets in the query space
//				Vector<IntersectionInformation> bucketInfos = bucketIndex.getAllBucketsInQuerySpace(querySpace);
//
//				double estimatedCount = 0;
//				// calculate the estimated count of results
//				for (IntersectionInformation bucketInfo : bucketInfos) {
//					estimatedCount += bucketInfo.getEstimatedBucketCount();
//				}
//
//				// if there are elements on the current neighbor
//				if (estimatedCount > 0.0) {
//					relevantSources.add(URLDecoder.decode(currEntry.getKey(), "utf-8"));
//				}
//			}

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
			int indexMapSize = index.getQTreesMap().entrySet().size();
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
	
	public static String DATA_DIR = "input/linked-data/";
	private static boolean printTrees = false;

	// get hash coordinates using the simple hasher
	public void testQTreeLinkedDataSimpleHasher() throws Exception {
		System.out.println("======\ntestQTreeLinkedDataSimpleHasher\n======");
		int minCharValue = 32;
		int maxCharValue = 252;
		
		QTreeHashing hasher = new SimpleHashing(minCharValue, maxCharValue); // all ASCII codes between these two 32 = whitespace, 126 = ~
			
		testQTreeLinkedData(hasher,printTrees,"simple");
	}

	// get hash coordinates using the cheating simple hasher
	public void testQTreeLinkedDataCheatedSimpleHasher() throws Exception {
		System.out.println("======\ntestQTreeLinkedDataCheatedSimpleHasher\n======");
		int minCharValue = 32;
		int maxCharValue = 252;
		
		int[] minValues = {2,69,-131};
		int[] maxValues = {183,169,183};

		QTreeHashing hasher = new CheatingSimpleHashing(minValues, maxValues, minCharValue, maxCharValue);
			
		testQTreeLinkedData(hasher,printTrees,"cheat");
	}

	// get hash coordinates using the UniStore prefix tree hasher
	public void testQTreeLinkedDataPrefixTreeHasher() throws Exception {
		System.out.println("======\ntestQTreeLinkedDataPrefixTreeHasher\n======");
		QTreeHashing hasher = new PrefixTreeHashing();
			
		testQTreeLinkedData(hasher,printTrees,"prefix");
	}

	// get hash coordinates using a very very simple hasher
	public void testQTreeLinkedDataNaiveHasher() throws Exception {
		System.out.println("======\ntestQTreeLinkedDataNaiveHasher\n======");
		QTreeHashing hasher = new MarioHashing();
			
		testQTreeLinkedData(hasher,printTrees,"naive");
	}
}
