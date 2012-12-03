package qtree;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.Variable;
import org.semanticweb.yars.nx.namespace.FOAF;
import org.semanticweb.yars.nx.namespace.RDF;

/**
 * This test case contains the queries and hardcoded query plans.
 *
 * @author aha
 *
 */
public class SampleQueries {
    
    
    public static Node[][] Q0 = new Node[][] {
	new Node[] { new Resource("http://www.w3.org/People/Berners-Lee/card#i"), FOAF.KNOWS, new Variable("p1") } ,
	new Node[] { new Variable("p1"), FOAF.KNOWS, new Variable("p2") } ,
	new Node[] { new Variable("p2"), FOAF.NAME, new Variable("hp") },
};

public static int[][] Q0J = { { 2, 0 }, { 5, 0 } };
    public static Node[][] Q1 = new Node[][] {
	new Node[] { new Variable("s"), RDF.TYPE, FOAF.AGENT } ,
	new Node[] { new Variable("s"), RDF.TYPE, FOAF.AGENT } ,
//	new Node[] { new Variable("s"), FOAF.KNOWS, new Variable("o") } 
};

public static int[][] Q1J = { { 0, 0 } };

public static Node[][] Q2 = new Node[][] {
	new Node[] { new Resource("http://www.w3.org/People/Berners-Lee/card#i"), FOAF.KNOWS, new Variable("p1") } ,
	new Node[] { new Variable("p1"), FOAF.KNOWS, new Variable("p2") } 
};

public static int[][] Q2J = { { 2, 0 } };

public static Node[][] Q3 = new Node[][] {
	new Node[] { new Resource("http://www.w3.org/People/Berners-Lee/card#i"), FOAF.KNOWS, new Variable("p1") } ,
	new Node[] { new Variable("p1"), FOAF.KNOWS, new Variable("p2") } ,
	new Node[] { new Variable("p2"), FOAF.HOMEPAGE, new Variable("hp") },
};

public static int[][] Q3J = { { 2, 0 }, { 5, 0 } };

public static Node[][] Q4 = new Node[][] {
	new Node[] { new Resource("http://www.w3.org/People/Berners-Lee/card#i"), FOAF.KNOWS, new Variable("s") } ,
	new Node[] { new Variable("s"), new Variable("p"), new Variable("o") } ,
};

public static int[][] Q4J = { { 2, 0 } };


public static Node[][] Q6 = new Node[][] {
	new Node[] { new Variable("s"), new Variable("p"), new Resource("http://my.opera.com/piallysmies/") } ,
	new Node[] { new Variable("s"), new Resource("http://xmlns.com/foaf/0.1/homepage"), new Variable("o") } ,
};
// 
public static int[][] Q6J = { { 0, 0 } };

public static Node[][] Q7 = new Node[][] {
	new Node[] { new Variable("s"), new Resource("http://xmlns.com/foaf/0.1/mbox_sha1sum"), new Literal("8549f68167b35eec0f36b740a946503333b6307d") } ,
	new Node[] { new Variable("s"), new Resource("http://xmlns.com/foaf/0.1/homepage"), new Resource("http://my.opera.com/MarkSchenk/") } ,
};

public static int[][] Q7J = { { 0, 0 } };

public static Node[][] Q5 = new Node[][] {
	new Node[] { new Variable("s"), new Resource("http://foaf.qdos.com/lastfm/schema/favouriteArtist"), new Resource("http://www.bbc.co.uk/music/artists/bd513de0-e42f-425e-ae46-817d7bc5fb1c#artist") } ,
	new Node[] { new Variable("s"), new Resource("http://xmlns.com/foaf/0.1/name"), new Literal("Bruno Bonfils") } ,
};

public static int[][] Q5J = { { 0, 0 } };

public static Node[][] QStar1= new Node[][] {
	new Node[] { new Variable("join"), new Resource("http://dbpedia.org/property/origin"), new Resource("http://dbpedia.org/resource/California") } ,
	new Node[] { new Variable("join"), new Resource("http://dbpedia.org/ontology/homeTown"), new Resource("http://dbpedia.org/resource/California") } //,
};

public static int[][] QStar1J = { { 0, 0 } };

public static Node[][] QStar2= new Node[][] {
	new Node[] { new Variable("join"), new Resource("http://www.w3.org/2000/01/rdf-schema#seeAlso"), new Resource("http://my.opera.com/SuperKoko/xml/foaf") } ,
	new Node[] { new Variable("join"), new Resource("http://xmlns.com/foaf/0.1/mbox_sha1sum"), new Literal("f52add90cb5e2211467165be8797dba26730dd57") } ,
	new Node[] { new Variable("join"), new Resource("http://xmlns.com/foaf/0.1/nick"), new Literal("SuperKoko") }
};

public static int[][] QStar2J = { { 0, 0 }, { 3, 0 } };
	
public static Node[][] QStar3= new Node[][] {
	new Node[] { new Variable("join"), new Resource("http://dbpedia.org/ontology/country"), new Resource("http://dbpedia.org/resource/Canada") } ,
	new Node[] { new Variable("join"), new Resource("http://dbpedia.org/property/country"), new Resource("http://dbpedia.org/resource/Canada") } ,
	new Node[] { new Variable("join"), new Resource("http://dbpedia.org/ontology/locationCountry"), new Resource("http://dbpedia.org/resource/Canada") }
};

public static int[][] QStar3J = { { 0, 0 }, { 3, 0 } };

public static Node[][] QStar4= new Node[][] {
	new Node[] { new Variable("join"), new Resource("http://xmlns.com/foaf/0.1/accountName"), new Literal("271900020") } ,
	new Node[] { new Variable("join"), new Resource("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), new Resource("http://xmlns.com/foaf/0.1/OnlineAccount") } ,
	new Node[] { new Variable("join"), new Resource("http://xmlns.com/foaf/0.1/accountServiceHomepage"), new Resource("http://www.facebook.com/") }
};

public static int[][] QStar4J = { { 0, 0 }, { 3, 0 } };

//public static Node[][][] QUERIES = { Q1, Q2, Q3, Q4 };
//public static int[][][] QJ = { Q1J, Q2J, Q3J, Q4J };

//public static Node[][][] QUERIES = { Q0 };
//public static int[][][] QJ = { Q0J };

public static Node[][][] QUERIES = {Q0, Q1, Q2, Q3, Q4 ,Q5, Q6, Q7 };
public static int[][][] QJ = { Q0J, Q1J, Q2J, Q3J, Q4J,Q5J, Q6J, Q7J };

//public static Node[][][] QUERIES = { QStar1, QStar2, QStar3, QStar4 };
//public static int[][][] QJ = { QStar1J, QStar2J, QStar3J, QStar4J };


public static Node[][] QStar5= new Node[][] {
	new Node[] { new Resource("http://identi.ca/yournetguru/foaf"), new Resource("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), new Variable("join1") } ,
	new Node[] { new Variable("join1"), new Resource("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), new Variable("join2") } ,
	new Node[] { new Variable("join2"), new Resource("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), new Variable("join3") }
};

public static int[][] QStar5J = { { 2, 0 }, { 0, 0 } };

//public static Node[][][] QUERIES = {Q0, Q1, Q2, Q3, Q4 ,Q5, Q6, Q7, QStar1, QStar2, QStar3, QStar4 };
//public static int[][][] QJ = { Q0J, Q1J, Q2J, Q3J, Q4J,Q5J, Q6J, Q7J, QStar1J, QStar2J, QStar3J, QStar4J };


//[BENCH] Query:
//    <http://identi.ca/yournetguru/foaf> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?join1 .
//    ?join1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?join2 .
//    ?join2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?join3 .
//  [BENCH] Join position:
//    [2, 0]
//    [5, 0]
//  java.lang.NullPointerException
//          at de.ilmenau.qtree.index.OnDiskOne4AllQTreeIndex.getRankOrderedSources(OnDiskOne4AllQTreeIndex.java:840)
//          at de.ilmenau.qtree.index.OnDiskOne4AllQTreeIndex.evaluateQuery(OnDiskOne4AllQTreeIndex.java:435)
//          at org.semanticweb.bench.JoinBenchmark.benchmark(JoinBenchmark.java:83)
//          at org.semanticweb.bench.menu.Joins.joinBenchmark(Joins.java:178)
//          at org.semanticweb.bench.menu.Joins.main(Joins.java:93)



//I could not calculate this query
public static Node[][] QSLOW = new Node[][] {
	new Node[] { new Resource("http://my.opera.com/blinkybill/xml/foaf#me"), new Resource("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), new Variable("join1")},
	new Node[] { new Variable("join1"),new Resource("http://www.w3.org/2002/07/owl#disjointWith"), new Variable("join2")},
		new Node[] { new Variable("join2"), new Resource("http://www.w3.org/2000/01/rdf-schema#isDefinedBy"), new Variable("join3")}};

	public static int[][] QSLOWJ = { { 2, 0 }, { 5, 0}};
	
	public static Node[][][] QUERIESSLOW = { QSLOW };
	public static int[][][] QJS = { QSLOWJ };
}
