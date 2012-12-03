package indexer;

import ie.deri.urq.realidy.index.SemQTree;
import ie.deri.urq.realidy.insert.Indexer;
import ie.deri.urq.realidy.insert.InsertCallback;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import junit.framework.TestCase;
import main.CONSTANTS;

import org.semanticweb.yars.nx.parser.ParseException;

public class HistofIndexerTest extends TestCase {

    
    public void testWebInsert() throws URISyntaxException, FileNotFoundException, IOException {
	File out = new File(CONSTANTS.TESTDIR,"indexer.test.webindex.qtree.ser");
	SemQTree sqt = SemQTree.createSemQTreeSingleQTreeIndex("mario", 10, 10, 0, 100, false);
//	CONSTANTS.setProxy("localhost", 3128);
	Indexer.insertFromWeb(new URI("http://umbrich.net/foaf.rdf"), sqt,1,0);
	
	sqt.serialiseIndexToFile(out);
	assertTrue(out.exists());
    }
    
    public void testInsertNXZ() throws ParseException, IOException{
	
	File out = new File(CONSTANTS.TESTDIR,"indexer.test.nxzindex.qtree.ser");
	 File f = new File("input/4_src.nq.gz");
	SemQTree sqt = SemQTree.createSemQTreeSingleQTreeIndex("mario", 10, 10, 0, 100, false);
	
	Indexer.insertFromNXZ(f, sqt);
	
	assertEquals(1128, sqt.getNoOfStmts());
	assertEquals(4, sqt.getNoOfSrc());
	sqt.serialiseIndexToFile(out);
	assertTrue(out.exists());
	
    }
    
 public void testInsertNXZ2() throws ParseException, IOException{
	
	File out = new File(CONSTANTS.TESTDIR,"indexer.test.nxzindex2.qtree.ser");
	 File f = new File("input/4_src.nq.gz");
	SemQTree sqt = SemQTree.createSemQTreeSingleQTreeIndex("mario", 10, 10, 0, 100, false);
	InsertCallback cb = new InsertCallback(sqt);
	Indexer.insertFromNXZ(f, cb);
	
	assertEquals(1128, sqt.getNoOfStmts());
	assertEquals(4, sqt.getNoOfSrc());
	sqt.serialiseIndexToFile(out);
	assertTrue(out.exists());
	
    }
}
