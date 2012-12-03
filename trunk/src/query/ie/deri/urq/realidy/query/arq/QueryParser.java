package ie.deri.urq.realidy.query.arq;

import java.io.Serializable;

import java.util.Arrays;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.Variable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QueryVisitor;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.Prologue;


public class QueryParser implements Serializable{
	private final static Logger log = LoggerFactory.getLogger(QueryParser.class);
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final OpBGPExtractorVisitor opext = new OpBGPExtractorVisitor();

	public Node[][] transform(String queryString){
		Query query = QueryFactory.create(queryString);
		return transform(query);
	}

	public Node[][] transform(Query query){
		
		Op op = Algebra.compile(query);
		return transform(op);
	}

	public Node[][] transform(Op op){
		opext.reset();
		OpWalker.walk(op, opext);
		return transform(opext.getOpBGP());
	}

	public Node[][] transform(OpBGP bgp) {
		Node [][] bgps = new Node[bgp.getPattern().getList().size()][3];
		for(int a =0 ; a < bgps.length; a++){
			bgps[a] = transformBGPToNX(bgp.getPattern().get(a));
		}
		return bgps;
	}

	public int[][] findJoins(Node[][] bgps) {
		int [][] joins = new int[bgps.length-1][2];
		log.debug("Determine join positions");
		Node [] join = bgps[0];
		log.debug(" Starting with {}", Nodes.toN3(join));
		for(int i = 1; i < bgps.length; i++){
			Node[] n2 = bgps[i];
			Node left = null;
			boolean found =false;
			for(int lpos = 0; lpos  < join.length; lpos++){
				left = join[lpos];
				if(!(left instanceof Variable)) continue;
				Node right = null;
				
				for(int rpos=0; rpos < n2.length; rpos++){
					right = n2[rpos];
					if(!(right instanceof Variable)) continue;
					
					if(left.equals(right)){
						joins[i-1] = new int []{lpos,rpos};
						log.debug("  left:   {}",Nodes.toN3(join));
						log.debug("  rigth:  {}",Nodes.toN3(n2));
						log.debug("  joinpos {}",Arrays.toString(joins[i-1]));
						found = true;
						break;
					}
				}
				if(found) break;
			}
			Node [] join1 = new Node[join.length+n2.length];
			int c =0;
			for(Node n: join)join1[c++]=n;
			for(Node n: n2)join1[c++]=n;
			join=join1;
			
		}
		return joins;
	}


	private Node[] transformBGPToNX(Triple t) {
		Node [] n = new Node[3];
		n[0] = tranformResourceToNX(t.getSubject());
		n[1] = tranformResourceToNX(t.getPredicate());
		n[2] = tranformResourceToNX(t.getObject());
		return n;
	}

	private Node tranformResourceToNX(com.hp.hpl.jena.graph.Node node) {
		if(node.isURI())
			return new org.semanticweb.yars.nx.Resource(node.toString());
		else if(node.isLiteral())
			return new org.semanticweb.yars.nx.Literal(node.getLiteral().toString());
		else if(node.isBlank())
			return new org.semanticweb.yars.nx.BNode(node.getBlankNodeLabel());
		else if(node.isVariable())
			return new org.semanticweb.yars.nx.Variable(node.getName());
		else return null;
	}

	public BasicPattern getBGPPattern(Op op) {
		OpWalker.walk(op, opext);
//		System.err.println("Found "+opext.getBasicPattern().getList().size()+" basic patterns");
		return opext.getBasicPattern(); 
	}
}
