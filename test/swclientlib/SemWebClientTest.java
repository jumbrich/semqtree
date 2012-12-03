package swclientlib;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;

import de.fuberlin.wiwiss.ng4j.semwebclient.SemanticWebClient;

/**
 * 
 */

/**
 * @author juum [juergen.umbrich@deri.org]
 *
 */
public class SemWebClientTest {

    public static void main(String[] args) {
	long start= System.currentTimeMillis();
	// Create a new Semantic Web client.
	SemanticWebClient semweb = new SemanticWebClient(); 
	 
//	// Specify the query.
//	Triple t = new Triple(Node.ANY, Node.createURI("http://xmlns.com/foaf/0.1/knows"), Node.ANY);
//
//	// Search for the triple
//	Iterator iter = semweb.find(t);
//
//	// Loop over all matching triples
//	while(iter.hasNext()){
//		SemWebTriple triple = (SemWebTriple) iter.next();
//		System.out.println(triple.toString());
//	}

//	
	// Specify the query.
	String queryString = 
	"PREFIX foaf: <http://xmlns.com/foaf/0.1/> " + 
	"PREFIX doap: <http://usefulinc.com/ns/doap#> " +   
	"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " + 
	"SELECT DISTINCT ?name " + 
	"WHERE {<http://umbrich.net/foaf.rdf#me> foaf:knows ?knows. 	" +
	"?knows foaf:name ?name . }"; 
	 
	// Execute the query and obtain results. 
	Query query = QueryFactory.create(queryString); 
	QueryExecution qe = QueryExecutionFactory.create(query, semweb.asJenaModel("default")); 
	ResultSet results = qe.execSelect(); 
	
	// Output query results.
	//Don't know why we need this command, but without the method call the semantic web client terminates immediately
	ResultSetFormatter.out(System.out, results, query);
	long end= System.currentTimeMillis();
	System.out.println("Time elapsed: "+(end-start)+" ms!");
	System.out.println("Total results: "+results.getRowNumber());
	/**
	 * (un)successfullyDereferencedURIs are list containing all the URLs
	 */
	System.out.println("Successfully dereferenced URIs: "+semweb.successfullyDereferencedURIs().size()); 
	System.out.println("Unsuccessfully dereferenced URIs: "+semweb.unsuccessfullyDereferencedURIs().size());
	semweb.close();
    }
}
