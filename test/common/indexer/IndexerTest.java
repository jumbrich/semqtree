package indexer;

import ie.deri.urq.realidy.index.SemQTree;
import ie.deri.urq.realidy.insert.Indexer;

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

import org.semanticweb.yars.nx.parser.ParseException;

public class IndexerTest extends TestCase {



	public void testInsertNXZ() throws ParseException, IOException{

		File out = new File("qtree1.ser");
		File f = new File("/Users/juum/Tmp/data.nq");
		String [] h = {"mixed"};
		for(String hash: h){
			System.out.println("\n\n\n\n");
			SemQTree sqt = SemQTree.createSemQTreeSingleMultiDimHistogramIndex(hash, 50000, 20, 0, 1000000, false);
			Indexer.insertFromNXZ(f, sqt);
			sqt.serialiseIndexToFile(out);
			////	MultiDimHistogram h = sqt.getAbstractIndex().
			//		 assertEquals(1128, sqt.getNoOfStmts());
			////	assertEquals(4, sqt.getNoOfSrc());
			//		 sqt.serialiseIndexToFile(out);
			//		 assertTrue(out.exists());
			//	
			//		 try {
			//			 QueryResultEstimation sq= sqt.evaluateQuery("SELECT ?s \nWHERE{ ?s <http://xmlns.com/foaf/0.1/weblog> <http://www.umbrich.net/blog> .\n}");
			//			 System.out.println(sq.getRelevantSourcesRanked());
			//		 } catch (QTreeException e) {
			//		// 	TODO Auto-generated catch block
			//			 e.printStackTrace();
			//	}
			//	}
		}
	}    
}
