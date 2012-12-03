import ie.deri.urq.realidy.query.arq.QueryParser;
import junit.framework.TestCase;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpVars;
import com.hp.hpl.jena.sparql.algebra.TransformBase;
import com.hp.hpl.jena.sparql.algebra.Transformer;
import com.hp.hpl.jena.sparql.algebra.op.OpSequence;
import com.hp.hpl.jena.sparql.algebra.op.OpTriple;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.util.NodeFactory;



public class ARQQueryParser extends TestCase{

	public static String query1 = "" +
			"SELECT ?s ?s1 ?o ?o1\n" +
			"WHERE{ _:bn ?p ?o .\n" +
			"       ?o ?p2 ?s2 .\n"+
			"       ?s2 ?p3 ?o1 .}\n";
	
	public void testParser() throws Exception {
		Query query = QueryFactory.create(query1);
		
		QueryParser p = new QueryParser();
		
		for(Node[] n : p.transform(query1)){
			System.out.println(Nodes.toN3(n));
		}
//		Op op = Algebra.compile(query);
//		System.out.println(op);
//		op = Algebra.optimize(op);
//		QueryParser p = new QueryParser();
//		Node [][] t = p.transform(op);
//		BasicPattern pat = p.getBGPPattern(op);
//		OpSequence s = OpSequence.create();
//		for(Triple tr: pat.getList()){
////			System.out.println(tr);
//			s.add(new OpTriple(tr));
//			System.out.println(OpVars.allVars(new OpTriple(tr)));
//		}
//		s.add(new OpTriple(new Triple(ResourceFactory.createResource().asNode(), ResourceFactory.createResource().asNode(), ResourceFactory.createResource().asNode())));
//		System.out.println(Transformer.transform(new TransformBase(), s));
//		System.out.println(s);
//		System.out.println(OpVars.patternVars(s));
//		for(Node[] n : t){
//			System.out.println(Nodes.toN3(n));
//		}
	} 
}

