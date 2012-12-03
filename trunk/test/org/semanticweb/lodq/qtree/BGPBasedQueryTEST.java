package org.semanticweb.lodq.qtree;

import ie.deri.urq.wods.query.QTreeDataset;
import ie.deri.urq.wods.query.QTreeQueryEngine;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Iterator;

import junit.framework.TestCase;

import org.openrdf.vocabulary.RDFS;
import org.semanticweb.yars.nx.Resource;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import de.ilmenau.datasum.index.AbstractIndex;
import de.ilmenau.datasum.index.OnDiskOne4AllQTreeIndex;
import de.ilmenau.datasum.index.SingleQTreeIndex;

public class BGPBasedQueryTEST extends TestCase {

    private AbstractIndex _index;

    @Override
    protected void setUp() throws Exception {
        // TODO Auto-generated method stub
        super.setUp();
        createQTree();
    }
    
    private void createQTree() {
	_index = AbstractIndex.loadIndex(new File("tmp/qtree4all--hmario_b50_f10_min1_max1000.ser"));
	
    }

    public void testQuery1() throws Exception {
	
	long time = System.currentTimeMillis();
	String queryString = "SELECT * WHERE { ?s <http://xmlns.com/foaf/0.1/knows> ?o . ?o "+new Resource(RDFS.SEEALSO).toN3()+" ?o2 . }" ;
//	String queryString = "SELECT * WHERE { ?s ?p ?o .}" ;
	// Parse
        Query query = QueryFactory.create(queryString);
//        StageGeneratorQTree msg = new StageGeneratorQTree(null);
//        ARQ.getContext().set(ARQ.stageGenerator, msg) ;
        QTreeQueryEngine.register();
	QTreeQueryEngine.setQTree(_index);
        QTreeDataset ds =  new QTreeDataset(1, 200,null);
	QueryExecution engine = QueryExecutionFactory.create(query,ds);
        try {
	    Iterator<QuerySolution> results = engine.execSelect() ;
	    System.out.println(results.hasNext());
	    for ( ; results.hasNext() ; ) {
	        QuerySolution soln = results.next() ;
	        System.out.println(soln.varNames().next());
	        System.out.println(soln);
	        System.out.println(soln.get("s"));
	    }
	} finally {
		engine.close() ;
	}
//	engine = QueryExecutionFactory.create(query, ModelFactory.createDefaultModel());
//	try {
//	    Iterator<QuerySolution> results = engine.execSelect() ;
//	    for ( ; results.hasNext() ; ) {
//	        QuerySolution soln = results.next() ;
//	    }
//	} finally {
//		engine.close() ;
//	}
	
	long time1 = System.currentTimeMillis();
    }
}
