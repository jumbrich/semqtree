package ie.deri.urq.realidy.query.qtree;

import ie.deri.urq.realidy.query.arq.QueryParser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.Triple;
import org.semanticweb.yars2.query.algebra.Operator;
import org.semanticweb.yars2.query.algebra.sparql.OpBGP;
import org.semanticweb.yars2.query.algebra.sparql.OpJoin;
import org.semanticweb.yars2.query.element.BasicGraphPattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QTreeQueryOptimiser {
	private final static Logger log = LoggerFactory.getLogger(QTreeQueryOptimiser.class);
	
	public static Integer []  optimise(Node [][] bgps, int[] noOfBuckets, int maxBuckets){
		
		OpBGP[] ops = new OpBGP[bgps.length];
		
		BasicGraphPattern p = new BasicGraphPattern();
		p.add(Triple.fromArray(bgps[0]));
		ops[0] = new OpBGP(p);	
		Operator op = ops[0];
		for(int i = 1; i < bgps.length;i++){
			p = new BasicGraphPattern();
			p.add(Triple.fromArray(bgps[i]));
			ops[i] = new OpBGP(p);
			op = new OpJoin(op, ops[i]);
		}
		if(op instanceof OpJoin && !((OpJoin)op).isJoin()) return null;
				
		
		return getBestPlan(ops,noOfBuckets,maxBuckets);
	}
	
	
	
	private static Integer[] getBestPlan(OpBGP[] ops, int[] noOfBuckets, int maxBuckets) {
		List<Integer> l = new ArrayList<Integer>(ops.length);

		//get the first element
		//   
		//TODO 
		//instead of map use recursion with costs and join 
		
		Map<OpJoin, Integer> joinCost =null;
		
		
		for(int i = 0; i < ops.length-1; i++){
			//calculate the join costs for all possibele joins
			joinCost = firstJoins(joinCost, ops,noOfBuckets,maxBuckets);
			//select the cheapest join
			OpJoin cheapest = findCheapestJoin(joinCost);
			
			//build the map for the next iteration
			//TODO this might be not the best code at all, and overhead to build the map
			Map<OpJoin, Integer> opJoinCosts =new HashMap<OpJoin, Integer>(1);
			if(cheapest!=null){
				opJoinCosts =new HashMap<OpJoin, Integer>(1);
				opJoinCosts.put(cheapest, joinCost.get(cheapest));
			}else{
				opJoinCosts =new HashMap<OpJoin, Integer>(0);
			}
				
			joinCost= opJoinCosts;
		}
		if(joinCost != null && joinCost.size()>0){
			OpJoin cheapest = findCheapestJoin(joinCost);
			l = convertPlanToOrder(cheapest,ops);
		}
		else if(joinCost !=null && joinCost.size()==0){
			l = new ArrayList<Integer>(0);
			return new Integer[0];
		}
		else
			l.add(0);
		
		Integer[] res = new Integer[noOfBuckets.length];
		
		return l.toArray(res);
		
////		System.exit(0);
//		
//		List<Integer> l1 = new ArrayList<Integer>();
//
//		int leftIDX = getMin(noOfBuckets, l1, ops,null);
//		l1.add(leftIDX);
//		Operator op = ops[leftIDX];
//		System.out.println("Join "+ops[leftIDX]);
//		while(l1.size()<ops.length){
//			//find cheapest join partner
//			int rightIDX = getMin(noOfBuckets,l1,ops, op);
//			if(rightIDX==-1) break;
//			l1.add(rightIDX);
//			System.out.println(" with "+ops[rightIDX]);
//			op = new OpJoin(op, ops[rightIDX]);
//		}
//		Long costs = calculateCosts(l,noOfBuckets);
		
//		Integer[] res = new Integer[noOfBuckets.length];
//		System.out.println("L1:"+l1);
//		System.out.println(l.equals(l1));
//		return l1.toArray(res);
		
	}

	
	
	
	private static List<Integer> convertPlanToOrder(Operator op, OpBGP[] ops) {
		List<Integer> list = new ArrayList<Integer>();
		if(op instanceof OpJoin){
			list = convertPlanToOrder(((OpJoin) op).getLeftOperator(),ops);
			if(((OpJoin) op).getRightOperator() instanceof OpBGP){
				list.add(findOpBGPIndex(ops, ((OpBGP) ((OpJoin) op).getRightOperator())));
			}
		}
		else{
//			System.out.println(op.getClass());
			list.add(findOpBGPIndex(ops, ((OpBGP) op)));
		}
		return list;
	}



	private static Integer findOpBGPIndex(OpBGP[] ops, OpBGP opBGP) {
		for(int i=0; i< ops.length; i++){
			if(ops[i].equals(opBGP))return i;
		}
		return -1000;
	}



	private static void printJoin(OpJoin op, int i) {
		Operator left = op.getLeftOperator();
		Operator right = op.getRightOperator();
//		System.out.println("left: "+left);
//		System.out.println("right: "+right);
	}



	private static OpJoin findCheapestJoin(Map<OpJoin, Integer> joinCost) {
		OpJoin cheapest = null;
		int min = Integer.MAX_VALUE;
		for(Entry<OpJoin,Integer> ent: joinCost.entrySet()){
			if(Math.min(min, ent.getValue())==ent.getValue()){
				min=ent.getValue(); 
				cheapest= ent.getKey();
			}
		}
		return cheapest;
	}



	private static Map<OpJoin, Integer> firstJoins(Map<OpJoin, Integer> joinCost,
			OpBGP[] ops, int[] noOfBuckets, int maxBuckets) {
		HashMap<OpJoin, Integer> res = new HashMap<OpJoin, Integer>();
		if(joinCost==null){
			for(int lpos =0; lpos < ops.length; lpos++){
				for(int rpos = lpos+1; rpos < ops.length; rpos++){
					OpJoin join = new OpJoin(ops[lpos], ops[rpos]);
					if(join.isJoin()){
						res.put(join, Math.min(maxBuckets,noOfBuckets[lpos]+noOfBuckets[rpos]));
					}
				}
			}
		}else{
			for(Entry<OpJoin,Integer> ent: joinCost.entrySet()){
				Set<OpBGP> bgps = getBGPs(ent.getKey());
				for(int rpos =0; rpos < ops.length; rpos++){
					if(bgps.contains(ops[rpos])) continue;
					OpJoin join = new OpJoin(ent.getKey(), ops[rpos]);
					if(join.isJoin()){
						res.put(join,Math.min(maxBuckets, ent.getValue()+noOfBuckets[rpos]));
					}
				}
			}
		}
		return res;
	}



	private static Set<OpBGP> getBGPs(Operator key) {
		Set<OpBGP> res = new HashSet<OpBGP>();
		if(key instanceof OpBGP){
			res.add((OpBGP)key);
		}
		if(key instanceof OpJoin){
//			System.out.println(key);
//			System.out.println(res);
//			System.out.println(((OpJoin) key).getLeftOperator());
//			System.out.println(((OpJoin) key).getRightOperator());
			res.addAll(getBGPs(((OpJoin)key).getLeftOperator()));
			res.addAll(getBGPs(((OpJoin)key).getRightOperator()));
		}
		return res;
	}



	private static int getMin(int[] noOfBuckets, List<Integer> l, OpBGP[] ops, Operator op) {
		int minIDx = -1, minValue = Integer.MAX_VALUE;
		for(int i =0; i <noOfBuckets.length; i++){
			if(l.contains(i)) continue;
			
			if(Math.min(minValue, noOfBuckets[i])==noOfBuckets[i] && (op==null || new OpJoin(op, ops[i]).isJoin())){
				minIDx=i;
				minValue = noOfBuckets[i]; 
			}
		}
		return minIDx;
	}

	public static String query1 = "" +
		"SELECT ?s ?s1 ?o\n" +
		"WHERE{ ?s <http://a.de/c> ?o .\n" +
		"       ?o ?p ?o2 .\n"+
		"       ?o2 <http://a.de/b> ?hp ." +
		"       ?twitter <http://a.de/a> ?hp .}\n";

//	 			
//	                                   TP2: ?o ?p ?o2                      [Max buckets]
//	                                   TP3: ?o2 foaf:homepage ?hp [20 buckets]
//	                                   TP4: ?twitter foaf:homepage ?hp . [30 buckets]
	
	
	public static void main(String[] args) {
		QueryParser p = new QueryParser();
		int [] b = {10,100000,20,40};
		Node [][] in = p.transform(query1);
//		for(int i = 0; i < in.length; i++)System.out.println(b[i]+" "+Nodes.toN3(in[i]));
//		
//		System.out.println("Reorder");
		Integer [] newOrder = QTreeQueryOptimiser.optimise(in,b, 10000000);
		Node[][] reorder = reorder(in,newOrder);
		for(Node[]n: reorder)System.out.println(Nodes.toN3(n));
	}
	

	private static Node[][] reorder(Node[][] bgps, Integer[] newJoinOrder) {
		Node [][] n = new Node[bgps.length][];
		for(int i=0; i < newJoinOrder.length;i++){
			n[i] = bgps[newJoinOrder[i]];
		}
		return n;
	}
}
