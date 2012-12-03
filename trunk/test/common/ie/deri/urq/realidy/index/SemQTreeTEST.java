package ie.deri.urq.realidy.index;

import ie.deri.urq.realidy.index.SemQTree;

import java.io.File;

import junit.framework.TestCase;
import main.CONSTANTS;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;

public class SemQTreeTEST extends TestCase {
    
    public void testInsertSaveLoadSemQTree() {
	SemQTree sqt = SemQTree.createSemQTreeSingleQTreeIndex("mario", 10, 10, 0, 100, true);
	
	Node s = new Resource("a");
	Node p = new Resource("p");
	Node o = new Resource("p");
	Node c = new Resource("c");
	Node [] stmt = new Node[]{s,p,o,c};
	sqt.addStatment(stmt);
	assertEquals(1, sqt.getNoOfStmts());
	
	//serialise
	File f = new File(CONSTANTS.TESTDIR,"test.qtree");
	f.deleteOnExit();
	sqt.serialiseQTreeToFile(f);
	
	assertTrue(f.exists());
	
	sqt = null;
	
	//deserialise
	sqt = SemQTree.loadIndex(f);
	assertNotNull(sqt);
	assertEquals(1, sqt.getNoOfStmts());
	
//	System.out.println(sqt.info());
	f.delete();
	assertFalse(f.exists());
    }
    
    public void testInsertSaveLoadMultiDim() {
    	SemQTree sqt = SemQTree.createSemQTreeSingleMultiDimHistogramIndex("mario", 10, 10, 0, 100, true);
    	
    	Node s = new Resource("a");
    	Node p = new Resource("p");
    	Node o = new Resource("p");
    	Node c = new Resource("c");
    	Node [] stmt = new Node[]{s,p,o,c};
    	sqt.addStatment(stmt);
    	assertEquals(1, sqt.getNoOfStmts());
    	
    	//serialise
    	File f = new File(CONSTANTS.TESTDIR,"test.qtree");
    	f.deleteOnExit();
    	sqt.serialiseQTreeToFile(f);
    	
    	assertTrue(f.exists());
    	
    	sqt = null;
    	
    	//deserialise
    	sqt = SemQTree.loadIndex(f);
    	assertNotNull(sqt);
    	assertEquals(1, sqt.getNoOfStmts());
    	
//    	System.out.println(sqt.info());
    	f.delete();
    	assertFalse(f.exists());
        }
}