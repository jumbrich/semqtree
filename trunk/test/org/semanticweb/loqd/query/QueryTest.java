package org.semanticweb.loqd.query;

import java.util.Iterator;

import junit.framework.TestCase;

import org.semanticweb.lods.query.direct.DatasetGraphWeb;
import org.semanticweb.lods.query.direct.DatasetWeb;
import org.semanticweb.lods.query.direct.GraphWeb;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;


public class QueryTest extends TestCase {
	public void testQuery1() throws Exception {
		long time = System.currentTimeMillis();

		String queryString = "SELECT * WHERE { <http://harth.org/andreas/foaf#ah> ?p ?o . }" ;
		Query query = QueryFactory.create(queryString) ;
		
		GraphWeb go = new GraphWeb();
		DatasetGraphWeb dgo = new DatasetGraphWeb(go);
		DatasetWeb dowl = new DatasetWeb(dgo);
		
		QueryExecution engine = QueryExecutionFactory.create(query, dowl) ;

		try {
		    Iterator<QuerySolution> results = engine.execSelect() ;
		    for ( ; results.hasNext() ; ) {
		        QuerySolution soln = results.next() ;
		        System.out.println(soln);
		    }
		} finally {
			engine.close() ;
		}
		
		long time1 = System.currentTimeMillis();

		System.err.println("query evaluated in " + (time1-time) + " ms");
	}
	
	public void testQuery2() throws Exception {
		long time = System.currentTimeMillis();

		String queryString = "SELECT ?sa WHERE { <http://harth.org/andreas/foaf#ah> <http://www.w3.org/2002/07/owl#sameAs> ?sa . }" ;
		Query query = QueryFactory.create(queryString) ;
		
		GraphWeb go = new GraphWeb();
		DatasetGraphWeb dgo = new DatasetGraphWeb(go);
		DatasetWeb dowl = new DatasetWeb(dgo);
		
		QueryExecution engine = QueryExecutionFactory.create(query, dowl) ;

		try {
		    Iterator<QuerySolution> results = engine.execSelect() ;
		    for ( ; results.hasNext() ; ) {
		        QuerySolution soln = results.next() ;
		        System.out.println(soln);
		    }
		} finally {
			engine.close() ;
		}
		
		long time1 = System.currentTimeMillis();

		System.err.println("query evaluated in " + (time1-time) + " ms");
	}
	
	public void testQuery3() throws Exception {
		long time = System.currentTimeMillis();

		String queryString = "SELECT ?name WHERE { <http://harth.org/andreas/foaf#ah> <http://www.w3.org/2002/07/owl#sameAs> ?sa . ?sa <http://xmlns.com/foaf/0.1/name> ?name . } "; 

		Query query = QueryFactory.create(queryString) ;
		
		GraphWeb go = new GraphWeb();
		DatasetGraphWeb dgo = new DatasetGraphWeb(go);
		DatasetWeb dowl = new DatasetWeb(dgo);
		
		QueryExecution engine = QueryExecutionFactory.create(query, dowl) ;

		try {
		    Iterator<QuerySolution> results = engine.execSelect() ;
		    for ( ; results.hasNext() ; ) {
		        QuerySolution soln = results.next() ;
		        System.out.println(soln);
		    }
		} finally {
			engine.close() ;
		}
		
		long time1 = System.currentTimeMillis();

		System.err.println("query evaluated in " + (time1-time) + " ms");
	}
	
	public void testQuery4() throws Exception {
		long time = System.currentTimeMillis();
		
		String queryString = "SELECT ?name WHERE { <http://harth.org/andreas/foaf#ah> <http://xmlns.com/foaf/0.1/knows> ?knows . ?knows <http://xmlns.com/foaf/0.1/name> ?name . } "; 

		Query query = QueryFactory.create(queryString) ;
		
		GraphWeb go = new GraphWeb();
		DatasetGraphWeb dgo = new DatasetGraphWeb(go);
		DatasetWeb dowl = new DatasetWeb(dgo);
		
		QueryExecution engine = QueryExecutionFactory.create(query, dowl) ;

		try {
		    Iterator<QuerySolution> results = engine.execSelect() ;
		    for ( ; results.hasNext() ; ) {
		        QuerySolution soln = results.next() ;
		        System.out.println(soln);
		    }
		} finally {
			engine.close() ;
		}
		
		long time1 = System.currentTimeMillis();

		System.err.println("query evaluated in " + (time1-time) + " ms");
	}

	public void testQuery5() throws Exception {
		long time = System.currentTimeMillis();
		
		String queryString = "SELECT ?label WHERE { " +
							" <http://harth.org/andreas/foaf#ah> <http://xmlns.com/foaf/0.1/topic_interest> ?int . " + 
							" ?int <http://www.w3.org/2000/01/rdf-schema#label> ?label . " +
							"  FILTER regex(?label, \"a\", \"i\")  } "; 

		Query query = QueryFactory.create(queryString) ;
		
		GraphWeb go = new GraphWeb();
		DatasetGraphWeb dgo = new DatasetGraphWeb(go);
		DatasetWeb dowl = new DatasetWeb(dgo);
		
		QueryExecution engine = QueryExecutionFactory.create(query, dowl) ;

		try {
		    Iterator<QuerySolution> results = engine.execSelect() ;
		    for ( ; results.hasNext() ; ) {
		        QuerySolution soln = results.next() ;
		        System.out.println(soln);
		    }
		} finally {
			engine.close() ;
		}
		
		long time1 = System.currentTimeMillis();

		System.err.println("query evaluated in " + (time1-time) + " ms");
	}

}
