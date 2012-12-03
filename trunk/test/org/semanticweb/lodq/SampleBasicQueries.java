package org.semanticweb.lodq;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.Variable;
import org.semanticweb.yars.nx.namespace.FOAF;
import org.semanticweb.yars.nx.namespace.RDF;
import org.semanticweb.yars.nx.namespace.RSS;

public class SampleBasicQueries {
	public static Node[][] QUERIES = new Node[][] {
		new Node[] { new Variable("s"), RDF.TYPE, FOAF.PERSON } ,
		new Node[] { new Variable("s"), FOAF.KNOWS, new Variable("o") } ,
		new Node[] { new Variable("s"), FOAF.KNOWS, new Resource("http://www.w3.org/People/Berners-Lee/card#i") } ,
		new Node[] { FOAF.PERSON, new Variable("p"), new Variable("o") } ,
		new Node[] { new Resource("http://www.w3.org/People/Berners-Lee/card#i"), FOAF.NAME, new Variable("o") } ,		
		new Node[] { new Resource("http://www.w3.org/People/Berners-Lee/card#i"), RDF.TYPE, RSS.ITEM } ,		
	};
}
