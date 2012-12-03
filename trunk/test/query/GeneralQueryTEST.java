import ie.deri.urq.realidy.index.SemQTree;
import ie.deri.urq.realidy.insert.Indexer;

import java.io.File;

import junit.framework.TestCase;
import main.CONSTANTS;

public class GeneralQueryTEST extends TestCase {

    private File input = new File("input/4_src.nq.gz");
    private File qtree = new File(CONSTANTS.TESTDIR,"query.test.general.qtree.ser");
    private File histo = new File(CONSTANTS.TESTDIR,"query.test.general.histo.ser");

    @Override
    protected void setUp() throws Exception {
    	super.setUp();
    	if(!CONSTANTS.TESTDIR.exists())CONSTANTS.TESTDIR.mkdirs();
		if(!qtree.exists()){
			SemQTree sqt = SemQTree.createSemQTreeSingleQTreeIndex("mario", 10, 10, 0, 100, false);
			Indexer.insertFromNXZ(input, sqt);
			sqt.serialiseIndexToFile(qtree);
		}
		if(!histo.exists()){
			SemQTree sqt = SemQTree.createSemQTreeSingleMultiDimHistogramIndex("mario", 10, 10, 0, 100, false);
			Indexer.insertFromNXZ(input, sqt);
			sqt.serialiseIndexToFile(histo);
		}
    }
    public void testLookupQueryQTree() throws Exception {
    	SemQTree sqt = SemQTree.loadIndex(qtree);

		String queryString = "SELECT * WHERE { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o2 . }" ;

		assertNotSame(0, sqt.getRelevantSourcesForQuery(queryString).size());
    }
    public void testLookupQueryMultiDim() throws Exception {
    	SemQTree sqt = SemQTree.loadIndex(histo);

		String queryString = "SELECT * WHERE { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o2 . }" ;

		assertNotSame(0, sqt.getRelevantSourcesForQuery(queryString).size());
    }
    
    public void testJoinQueryQTree() throws Exception {
    	SemQTree sqt = SemQTree.loadIndex(qtree);

    	String queryString = "SELECT * WHERE { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o2 . \n?s <http://xmlns.com/foaf/0.1/knows> ?o3 .}" ;

    	assertNotSame(0, sqt.getRelevantSourcesForQuery(queryString).size());
    }
    
    public void testJoinQueryMultiDimHisto() throws Exception {
    	SemQTree sqt = SemQTree.loadIndex(histo);

    	String queryString = "SELECT * WHERE { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o2 . \n?s <http://xmlns.com/foaf/0.1/knows> ?o3 .}" ;

    	assertNotSame(0, sqt.getRelevantSourcesForQuery(queryString).size());
        }
    
}