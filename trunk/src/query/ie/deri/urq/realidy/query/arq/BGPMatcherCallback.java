package ie.deri.urq.realidy.query.arq;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.Variable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.AnonId;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.DatasetGraph;

public class BGPMatcherCallback implements org.semanticweb.yars.nx.parser.Callback{
	private final static Logger log = LoggerFactory.getLogger(BGPMatcherCallback.class);
	//    Node[] _bgp;
	List<Node[]> _t;
	AtomicInteger _var = new AtomicInteger(0);
	private DatasetGraph _ds;
	private QueryBenchmark _bench;

	public BGPMatcherCallback(DatasetGraph dataset, BasicPattern bp) {
		this(dataset,bp,null);
	}

	public BGPMatcherCallback(DatasetGraph dataset, BasicPattern bp,
			QueryBenchmark bench) {
		_t = new ArrayList<Node []>();
		_ds = dataset;
		_bench = bench;
		
		for (Triple t: bp.getList()){
			Node [] bgp = new Node[3];		
			boolean a = add(t,bgp);
			if(a){
				for(Node n: bgp) System.out.println(n.toString());
				_t.add(bgp);
				log.info("Add "+Nodes.toN3(bgp)+ " to the filter");
			}
						
			
			
		}
	}

	private boolean add(Triple t, Node[] bgp) {
		if(!add(t.getSubject(),bgp,0)) return false;
		if(!add(t.getPredicate(),bgp,1)) return false;
		if(!add(t.getObject(),bgp,2)) return false;
//		System.out.println(bgp[0]);
//		System.out.println(bgp[1]);
//		System.out.println(bgp[2]);
		return true;
	}

	private boolean add(com.hp.hpl.jena.graph.Node node, Node []nodeTrans, int i ) {
		if (node.isURI()) {
			nodeTrans[i] = new Resource(node.getURI());
		} else if (node.isVariable()) {
			nodeTrans[i] = new Variable("v"+_var.getAndIncrement());
		} else if (node.isLiteral()) {
			nodeTrans[i] = new Literal(node.getLiteral().toString());;
		}
		return true;
	}

	boolean match(Node[] nx) {
//		return true;
//		iterate over the list of all bgp's
		for(Node [] bgp: _t){
			//check if we match this bgp
			
			boolean tmp = true;
			for (int i = 0; i < bgp.length; i++) {
				if (!(bgp[i] instanceof Variable)) {
					if (!bgp[i].equals(nx[i])) {
						tmp = false;
					}
				}
			}
//			System.out.println(tmp +" for "+Nodes.toN3(bgp)+ " and "+Nodes.toN3(nx));
			if(tmp) return true;
		}
		return false;
	}

	public void endDocument() {
		// TODO Auto-generated method stub

	}

	public void processStatement(Node[] stmt) {
		//	System.out.println("process stmt "+Nodes.toN3(stmt)+" match:"+match(stmt));
		if (match(stmt)) {
			Triple t = new Triple(transformToNode(stmt[0]), transformToNode(stmt[1]),transformToNode(stmt[2]));
			_ds.getDefaultGraph().add(t);
			if(_bench!=null) _bench.addSrc(stmt[3]);
		}
	}


	com.hp.hpl.jena.graph.Node transformToNode(Node n ){
		if (n instanceof Resource) {
			return com.hp.hpl.jena.graph.Node.createURI(((Resource)n).toString());
		} else if (n instanceof Literal) {
			return com.hp.hpl.jena.graph.Node.createLiteral(((Literal)n).toString());
		} else if (n instanceof BNode) {
			return com.hp.hpl.jena.graph.Node.createAnon(new AnonId(((BNode)n).toString()));
		}
		return null; 
	}

	public void startDocument() {
		// TODO Auto-generated method stub

	}
}