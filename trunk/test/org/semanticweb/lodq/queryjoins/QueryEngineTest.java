package org.semanticweb.lodq.queryjoins;


import junit.framework.TestCase;

import org.semanticweb.lods.query.joins.QueryEngine;
import org.semanticweb.yars.util.CallbackNQOutputStream;

public class QueryEngineTest extends TestCase {
	public void testQuery1() {
		String query = "SELECT * WHERE { <http://harth.org/andreas/foaf#ah> <http://xmlns.com/foaf/0.1/knows> ?o . }" ;

		QueryEngine qe = new QueryEngine();
		
		qe.setOutputCallback(new CallbackNQOutputStream(System.out));

		qe.evaluate(query);
	}

	public void testQuery2() {
		String query = "SELECT * WHERE { <http://harth.org/andreas/foaf#ah> ?p ?o . ?o ?p2 ?o2 . }" ;

		QueryEngine qe = new QueryEngine();
		
		qe.setOutputCallback(new CallbackNQOutputStream(System.out));

		qe.evaluate(query);
	}
}
