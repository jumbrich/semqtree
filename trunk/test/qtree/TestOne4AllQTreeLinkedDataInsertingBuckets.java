package qtree;

import ie.deri.urq.wods.hashing.CheatingSimpleHashing;
import ie.deri.urq.wods.hashing.QTreeHashing;
import ie.deri.urq.wods.hashing.SimpleHashing;
import ie.deri.urq.wods.hashing.us.MarioHashing;
import ie.deri.urq.wods.hashing.us.PrefixTreeHashing;

import java.io.File;
import java.util.Iterator;

import junit.framework.TestCase;

import org.semanticweb.wods.indexer.IndexerManager;
import org.semanticweb.wods.indexer.InsertCallback;
import org.semanticweb.wods.indexer.OnDiskQTreeIndexerFactory;
import org.semanticweb.wods.indexer.Queue;

import de.ilmenau.datasum.index.Bucket;
import de.ilmenau.datasum.index.OnDiskOne4AllQTreeIndex;
import de.ilmenau.datasum.index.qtree.QTreeBucket;
import de.ilmenau.datasum.index.qtree.QTreeNode;
import de.ilmenau.datasum.index.update.UpdateBucket;

public class TestOne4AllQTreeLinkedDataInsertingBuckets extends TestCase {
	
	public static String DATA_DIR = "input/linked-data/";
	private static boolean printTrees = false;
	
	boolean storeDetailedCounts = false;
	
	
	public void testOne4allQTreeLinkedDataInsertingBuckets(QTreeHashing hasher, boolean printQTrees, String hasherName) throws Exception {
	
		String dataDir = "input/linked-data/";
	
	    OnDiskOne4AllQTreeIndex index = new OnDiskOne4AllQTreeIndex(hasher,storeDetailedCounts);
	    
	    File f = new File("output.qtree4all/qtree4all--"+index.getLabel()+".ser");
	    if (!f.exists()){
	    
		    Queue queue = new Queue(new File(dataDir));
		    IndexerManager manager = new IndexerManager(index, 1, queue, new OnDiskQTreeIndexerFactory(new InsertCallback(index)));
		    manager.runIndexer();
	    
		    long qtreeOnDiskSize = index.serialiseQTree(new File("output.qtree4all"));
	    } else {
	    	index.loadQTreeIndex(new File("output.qtree4all"));
	    	
	    	//System.out.println(index.getQTree().getStateString());
	    }
	    
	    
	    
	    // test inserting these buckets into a new QTree
	    
	    OnDiskOne4AllQTreeIndex newIndex = new OnDiskOne4AllQTreeIndex(hasher,storeDetailedCounts);
	    
	    for (Bucket currBucket : index.getQTree().getAllBuckets()){
	    	UpdateBucket insertBucket = 
	    		new UpdateBucket(currBucket.getLowerBoundaries(),
	    				currBucket.getUpperBoundaries(),
	    				currBucket.getCount(),
	    				currBucket.getAttributeNames(),
	    				Integer.parseInt(currBucket.getPeerID()),
	    				Integer.parseInt(currBucket.getNeighborID()),
	    				currBucket.getLocalBucketID());
	    	insertBucket.setSourceIDs(currBucket.getSourceIDs());
	    	
	    	System.out.println("Insert Bucket "+insertBucket.toString());
	    	
	    	newIndex.addBucket(insertBucket, true); // allowing buckets to be merged upon insertion
	    	
	    	
			// test if sourceIDs are inserted correctly
			boolean error = false;
			for (QTreeBucket currBuck: newIndex.getQTree().getAllBuckets()){
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
						error = error || !contained;
						if (error) {
							System.err.println("currNode (LID "+currNode.getLocalBucketID()+"): "+currNode.getBoundsString());
							break;
						}
						currNode = currNode.getParent();
					}
					if (error) {
						System.err.println(", currSourceID: "+currSourceID);
						break;
					}
				}
				if (error) {
					System.err.println(", currBucket (LID "+currBuck.getLocalBucketID()+"): "+currBuck.getBoundsString());
					break;
				}
			}
			if (error) {
				//System.err.println(newIndex.getQTree().getStateString());
				//System.exit(0);
				throw new Exception();
			} else {
				System.out.println("passed containment test");
			}
			
	    }
	    
	    
	    //test if the counts of all buckets are correct
	    double sumCountIndex = 0.0;
	    for (Bucket currBucket: index.getQTree().getAllBuckets()){
	    	sumCountIndex += currBucket.getCount();
	    }
	    
	    double sumCountIndexNew = 0.0;
	    for (Bucket currBucket: newIndex.getQTree().getAllBuckets()){
	    	sumCountIndexNew += currBucket.getCount();
	    }	    
	    
	    if (sumCountIndex != sumCountIndexNew){
	    	System.err.println("Counter error! old index: "+sumCountIndex + "new index: "+sumCountIndexNew);
	    	throw new Exception();
	    }
	    index = null;
	    
	    
	    
		/*long time = System.currentTimeMillis();

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
		System.out.println("time elapsed standard query evaluation " + stan + " ms");*/
		
		
	}
	
	// get hash coordinates using the simple hasher
	public void testQTreeLinkedDataSimpleHasher() throws Exception {
		System.out.println("======\ntestQTreeLinkedDataSimpleHasher\n======");
		int minCharValue = 32;
		int maxCharValue = 252;
		
		QTreeHashing hasher = new SimpleHashing(minCharValue, maxCharValue); // all ASCII codes between these two 32 = whitespace, 126 = ~
			
		testOne4allQTreeLinkedDataInsertingBuckets(hasher,printTrees,"simple");
	}
	
	
	// get hash coordinates using the cheating simple hasher
	public void testQTreeLinkedDataCheatedSimpleHasher() throws Exception {
		System.out.println("======\ntestQTreeLinkedDataCheatedSimpleHasher\n======");
		int minCharValue = 32;
		int maxCharValue = 252;
		
		int[] minValues = {2,69,-131};
		int[] maxValues = {183,169,183};
	
		QTreeHashing hasher = new CheatingSimpleHashing(minValues, maxValues, minCharValue, maxCharValue);
			
		testOne4allQTreeLinkedDataInsertingBuckets(hasher,printTrees,"cheat");
	}
	
	// get hash coordinates using the UniStore prefix tree hasher
	public void testQTreeLinkedDataPrefixTreeHasher() throws Exception {
		System.out.println("======\ntestQTreeLinkedDataPrefixTreeHasher\n======");
		QTreeHashing hasher = new PrefixTreeHashing();
			
		testOne4allQTreeLinkedDataInsertingBuckets(hasher,printTrees,"prefix");
	}
	
	// get hash coordinates using a very very simple hasher
	public void testQTreeLinkedDataNaiveHasher() throws Exception {
		System.out.println("======\ntestQTreeLinkedDataNaiveHasher\n======");
		QTreeHashing hasher = new MarioHashing();
			
		testOne4allQTreeLinkedDataInsertingBuckets(hasher,printTrees,"naive");
	}

}
