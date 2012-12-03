package swclientlib;

import java.util.HashSet;
import java.util.Set;

import junit.framework.TestCase;

import org.semanticweb.lodq.SampleJoinQueries;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.Variable;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;

import de.fuberlin.wiwiss.ng4j.semwebclient.SemanticWebClient;

public class SWCTestBGPJoin extends TestCase {
	public static String DATA_DIR = "input/linked-data/";
	
	
	public void testBGP() throws Exception {
		long time = System.currentTimeMillis();
		
		for (int i = 0; i < SampleJoinQueries.QUERIES.length; i++) {			
			Node[][] q = SampleJoinQueries.QUERIES[i];
			
			String query = "WHERE {";
			Set<Node> vars = new HashSet<Node>();
			for(Node[] nodes: q){
			    query+= Nodes.toN3(nodes)+"\n";
			    for(Node n: nodes){
				if(n instanceof Variable){
				    vars.add(n);
				}
			    }
			}
			query+=" }";
			String select ="SELECT ";
			for(Node var: vars){
			    select+= var.toN3()+" ";
			}
			select+= "\n";
			executeQuery(select+query);
//			System.out.println(query);
			break;
			
		}
		
		long time1 = System.currentTimeMillis();

		System.err.println("time elapsed: " + (time1-time) + " ms");
	}


	/**
	 * @param query
	 * @throws InterruptedException 
	 */
	private void executeQuery(String queryString) throws InterruptedException {
	    System.err.println("\nExecuting query:");
	    Thread.sleep(500);
	    System.out.println(queryString);
	    
	    long start= System.currentTimeMillis();
	    SemanticWebClient semweb = new SemanticWebClient(); 
	    Query query = QueryFactory.create(queryString); 
		QueryExecution qe = QueryExecutionFactory.create(query, semweb.asJenaModel("default")); 
		ResultSet results = qe.execSelect(); 
		
		
		
		// Output query results.
		//Don't know why we need this command, but without the method call the semantic web client terminates immediately
		System.err.println("\nResults\n");
		ResultSetFormatter.out(System.err, results, query);
		long end= System.currentTimeMillis();
		System.err.println("Time elapsed: "+(end-start)+" ms!");
		System.err.println("Total results: "+results.getRowNumber());
		System.err.println("Successfully dereferenced URIs: "+semweb.successfullyDereferencedURIs().size()); 
		System.err.println("Unsuccessfully dereferenced URIs: "+semweb.unsuccessfullyDereferencedURIs().size());
		semweb.close();
	}
}
