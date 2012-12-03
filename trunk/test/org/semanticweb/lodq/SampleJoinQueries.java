package org.semanticweb.lodq;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.Variable;
import org.semanticweb.yars.nx.namespace.FOAF;
import org.semanticweb.yars.nx.namespace.OWL;
import org.semanticweb.yars.nx.namespace.RDF;

/**
 * This test case contains the queries and hardcoded query plans.
 *
 * @author aha
 *
 */
public class SampleJoinQueries {
	public static Node[][] Q1 = new Node[][] {
		new Node[] { new Variable("s"), RDF.TYPE, FOAF.AGENT } ,
		new Node[] { new Variable("s"), FOAF.HOMEPAGE, new Variable("o") } 
	};

	public static int[][] Q1J = { { 0, 0 } };

	public static Node[][] Q2 = new Node[][] {
		new Node[] { new Resource("http://www.w3.org/People/Berners-Lee/card#i"), FOAF.KNOWS, new Variable("p1") } ,
		new Node[] { new Variable("p1"), new Resource(FOAF.NS + "interest"), new Variable("p2") } 
	};

	public static int[][] Q2J = { { 2, 0 } };

	public static Node[][] Q3 = new Node[][] {
		new Node[] { new Resource("http://www.w3.org/People/Berners-Lee/card#i"), FOAF.KNOWS, new Variable("p1") } ,
		new Node[] { new Variable("p1"), FOAF.KNOWS, new Variable("p2") } ,
		new Node[] { new Variable("p2"), FOAF.MADE, new Variable("hp") },
	};

	public static int[][] Q3J = { { 2, 0 }, { 5, 0 } };

	public static Node[][] Q4 = new Node[][] {
		new Node[] { new Resource("http://www.w3.org/People/Berners-Lee/card#i"), FOAF.KNOWS, new Variable("s") } ,
		new Node[] { new Variable("s"), new Variable("p"), new Variable("o") } ,
	};

	public static int[][] Q4J = { { 2, 0 } };

	public static Node[][] Q5 = new Node[][] {
		new Node[] { new Resource("http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB01273"), new Resource("http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/possibleDiseaseTarget"), new Variable("disease") },
		new Node[] { new Variable("disease"), OWL.SAMEAS, new Variable("sameDisease") },
		new Node[] { new Variable("altMedicine"), new Resource("http://purl.org/net/tcm/tcm.lifescience.ntu.edu.tw/treatment"), new Variable("sameDisease") },
		new Node[] { new Variable("altMedicine"), RDF.TYPE, new Resource("http://purl.org/net/tcm/tcm.lifescience.ntu.edu.tw/Medicine") } ,
	};
	
	public static Node[][] QSLOW = new Node[][] {
	new Node[] { new Resource("http://my.opera.com/blinkybill/xml/foaf#me"), new Resource("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), new Variable("join1")},
	new Node[] { new Variable("join1"),new Resource("http://www.w3.org/2002/07/owl#disjointWith"), new Variable("join2")},
		new Node[] { new Variable("join2"), new Resource("http://www.w3.org/2000/01/rdf-schema#isDefinedBy"), new Variable("join3")}};

	public static int[][] QSLOWJ = { { 2, 0 }, { 5, 0}};
	
	public static Node[][][] QUERIESSLOW = { QSLOW };
	public static int[][][] QJS = { QSLOWJ };

	
	public static int[][] Q5J = { { 2, 0 }, { 5, 2 }, { 6, 0 } };

	public static Node[][][] QUERIES = { Q1, Q2, Q3, Q4, Q5 };
	public static int[][][] QJ = { Q1J, Q2J, Q3J, Q4J, Q5J };

}
