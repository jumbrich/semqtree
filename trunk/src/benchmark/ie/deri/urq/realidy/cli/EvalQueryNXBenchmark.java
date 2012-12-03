package ie.deri.urq.realidy.cli;

import ie.deri.urq.realidy.index.IndexFactory;
import ie.deri.urq.realidy.index.IndexInterface;
import ie.deri.urq.realidy.query.arq.QueryParser;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.Variable;
import org.semanticweb.yars.nx.parser.NxParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.AnonId;
import com.hp.hpl.jena.rdf.model.Model;

import de.ilmenau.datasum.index.QueryResultEstimation;

public class EvalQueryNXBenchmark extends CLIObject{
	private final static Logger log = LoggerFactory.getLogger(EvalQueryNXBenchmark.class);
	private final QueryParser qp = new QueryParser();
	
	@Override
	protected void addOptions(Options opts) {
		//each benchmark needs an output file
		opts.addOption(CLIObject.OPTION_GENERIC_OUTPUT);
		opts.addOption(CLIObject.OPTION_GENERIC_INPUT);
		opts.addOption(CLIObject.OPTION_QUERY_FILE);
		opts.addOption(CLIObject.OPTION_REQUIRED_LOAD_INDEX);
	}
	@Override
	protected void execute(CommandLine cmd) {
		File outDir = new File(cmd.getOptionValue(CLIObject.PARAM_GENERIC_OUTPUT));
		if(!outDir.exists()) outDir.mkdirs();
		File query = new File(cmd.getOptionValue(CLIObject.PARAM_QUERY_FILE));
		log.info("Starting bechmark");
		File index = new File(cmd.getOptionValue(CLIObject.PARAM_LOAD_INDEX));
		IndexInterface idx = IndexFactory.loadIndex(index);
		
		long start = System.currentTimeMillis();
		File input = new File(cmd.getOptionValue(CLIObject.PARAM_GENERIC_INPUT));
		
		log.info("Time "+(System.currentTimeMillis()-start)+" ms");
		try {
			
			File statsFile = new File(outDir,idx.getLabel()+"."+query.getName()+".nxEval.stats");
			FileOutputStream fis;
			if(!statsFile.exists()){
				 fis = new FileOutputStream(statsFile);				
				 //what do we want to measure
				 fis.write(("#queryNo " +
						 "totalQueryTime " +
						 "jointime " +
						 "ranktime " +
						 "buckets " +
						 "realSources " +
						 "estSources " +
						 "realStmt " +
						 "realStmtTime " +
						 "qtreeAllStmt " +
						 "qtreeAllStmtPerc " +
						 "qtreeAllStmtTime " +
						 "top10Stmt " +
						 "top10StmtPerc " +
						 "top10StmtTime " +
						 "top50Stmt " +
						 "top50StmtPerc " +
						 "top50StmtTime " +
						 "top100Stmt " +
						 "top100StmtPerc " +
						 "top100StmtTime " +
						 "top200Stmt " +
						 "top200StmtPerc " +
						 "top200StmtTime " +
						 "maxK "+
				 "\n").getBytes());
			}else{
				 fis = new FileOutputStream(statsFile,true);
			}
			File [] queries = query.listFiles();
			for(File f: queries){
				if(f.isDirectory() || !f.getName().startsWith(idx.getLabel()))continue;
				try {
					File srcDumpFile = new File(outDir, f.getName()+"."+idx.getLabel()+".srcs");
					evaluate(f, srcDumpFile,input, fis);
					System.gc();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			fis.flush();fis.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	 private  Map<Node, List<Node[]>> getSourceMap(File data, Node[][] bgps) {
		Map<Node,List<Node[]>> srcMap = new java.util.HashMap<Node, List<Node[]>>();
		int counter = 0;
		InputStream in;
		try {
			in = new FileInputStream(data);
			 if(data.getName().endsWith(".gz"))
			    	in = new GZIPInputStream(in);
			 NxParser p = new NxParser(in,false,false);

			 Node[] nodes;
			 while(p.hasNext()){
				 counter++;
				 nodes =p.next();
				 if(match(nodes, bgps)){
					 if(!srcMap.containsKey(nodes[3])){
						 srcMap.put(nodes[3], new ArrayList<Node[]>());
					 }
					 srcMap.get(nodes[3]).add(
						 Arrays.copyOf(nodes, 3)
						 );
				 }
				 if(counter % 100000==0){
					 System.err.println("Parsed "+counter+" stms!");
				 }
			 }
			 in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return srcMap;
    }
	
	private  QueryResultEstimation deserialise(File serQueryFile) throws IOException, ClassNotFoundException {
		log.info("Deserialise "+serQueryFile);
		FileInputStream fis = new FileInputStream(serQueryFile);
		ObjectInputStream ois = new ObjectInputStream(fis);
		Object o = ois.readObject();
		ois.close();
		fis.close();

		return (QueryResultEstimation) o;
	}

	private void evaluate(File f, File srcDumpFile, File input,   FileOutputStream fis) throws IOException {
		QueryResultEstimation est;
		try {
			est = deserialise(f);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			return;
		}
		
		log.info("Evaluating the result of query "+f.getAbsolutePath()+"\n"+est);
		
		String name = f.getName();
		String [] tokens = name.split("\\.");
//		log.info("tokens are of length {}",tokens.length);
		String numeric = tokens[tokens.length-3];
		int queryCount =  Integer.valueOf(numeric);
		
		
    	//what do we want to measure
    	long totalQueryTime;
    	
    	long realSources;
    	long estSources;
    	long realStmt;
    	long realStmtTime;
    	long qtreeAllStmt;
    	double qtreeAllStmtPerc;
    	long qtreeAllStmtTime;
    	long maxK;
	    
    	Integer[] joinOrder = est.getJoinOrder();
    	int[][] joins = est.getJoinIndices();
    	Node [][] bgps = qp.transform(est.getQueryString());
    	
    	Map<Node,List<Node[]>> srcMap = getSourceMap(input,bgps);
    	
    	//get initial triplesd
    	long start = System.currentTimeMillis();
    	List<Node[]> current = computeJoin(joinOrder,joins,bgps, srcMap, srcMap.keySet());
    	realStmtTime = System.currentTimeMillis() - start;
    	//get the real sources and stmts as golden standard
    	
		dumpSources("real", countSources(current),srcDumpFile);
    	realSources = countSources(current).size();
    	realStmt = countStmts(current);
    	log.info("[Q-"+queryCount+"] REAL src: "+realSources+ " stms:"+realStmt+" time:"+realStmtTime);
    	current = null;
    	System.gc();System.gc();
     	//cleanup
    	
    	//QTree says we need to query X relevant sources
    	estSources = est.getRelevantSourcesRanked().size();
    	//it took how long to evaluate the join
    	totalQueryTime = est.getTotalQueryTime();
    	
    	
    	List<Node> rankOrderedSources = convertToNodeSet(est.getRelevantSourcesRanked());
    	start = System.currentTimeMillis();
    	//how long does it take to evaluate this query in memory
    	current = computeJoin(joinOrder, joins, bgps, srcMap, rankOrderedSources);
    	qtreeAllStmtTime= System.currentTimeMillis()-start;
    	
    	dumpSources("est",rankOrderedSources,srcDumpFile);
    	
    	qtreeAllStmt = countStmts(current);
    	if(current.size()!=0){
    		qtreeAllStmtPerc = qtreeAllStmt/(double)realStmt;	
    	}else{qtreeAllStmtPerc=0D;}
    	 
    	log.info("[Q-"+queryCount+"] QTree src: "+estSources+ " qtreeQueryTime: "+totalQueryTime+" stms:"+ qtreeAllStmt +" ("+qtreeAllStmtPerc+") time:"+qtreeAllStmtTime);
    	
    	
    	maxK = getMaxK(rankOrderedSources , countSources(current));
    	current = null;
    	System.gc();System.gc();
    	System.gc();
    	
    	//: result.getRelevantSourcesRanked();
    	// the k values for evaluating top-k
    	int[] topK = new int[]{10,50,100,200};
    	long[] topKTimes = new long[topK.length];
    		
    	// to store the percentages of each top-k result set
    	double[] topKPerc = new double[topK.length];
    	double[] topKPrec = new double[topK.length];

    	
    	// handle each of the k values
    	for (int k=0; k<topK.length; ++k) {
   			List<Node> topKSrcs = getTopKSources( topK[k], rankOrderedSources);
   			dumpSources("top-"+topK[k],topKSrcs,srcDumpFile);
   			
   			start = System.currentTimeMillis();
   			//how long does it take to evaluate this query in memory
   			List<Node[]> tmp = computeJoin(joinOrder,joins,bgps, srcMap, topKSrcs);
   			
   	    	topKTimes[k]= (System.currentTimeMillis()-start);
   	   		int stmts= countStmts(tmp);
   			topKPerc[k]= (double)stmts/(double)realStmt;
   			topKPrec[k] = getTopKPrecision(topKSrcs,countSources(tmp));
   			log.info("[Q-"+queryCount+"] QTree top-"+topK[k]+" src: "+countSources(tmp).size()+ " qtreeQueryTime: "+topKTimes[k]+" stms:"+ stmts +" ("+topKPerc[k]+") srcPrec:"+	topKPrec[k]);
   			tmp =null;
   			System.gc();System.gc();
    	}
    	
    	StringBuffer sb = new StringBuffer(""+queryCount).append(" ");
    	
    	int [] joinBuckets = est.getJoinResultingBuckets();
    	int joinBucket = 0;
		if(joinBuckets.length!=0)
			joinBucket = joinBuckets[joinBuckets.length-1];
		else if(est.getBgpResultingBuckets()!=null && est.getBgpResultingBuckets().length==1){
			joinBucket = est.getBgpResultingBuckets()[0];
		}
    	
		long joinEval = 0L;
		if(joinBuckets.length!=0)
		  joinEval = est.getJoinEvalTimes()[joinBuckets.length-1];
		else if(est.getBgpEvalTimes().length>0){
			joinEval = est.getBgpEvalTimes()[0];
		}
		sb.append(totalQueryTime).append(" ");
    	sb.append(joinEval).append(" ");
    	sb.append(est.getRankTime()).append(" ");
    	sb.append(joinBucket).append(" ");
    	sb.append(realSources).append(" ");
    	sb.append(estSources).append(" ");
    	sb.append(realStmt).append(" ");
    	sb.append(realStmtTime).append(" ");
    	sb.append(qtreeAllStmt).append(" ");
    	sb.append(qtreeAllStmtPerc).append(" ");
    	sb.append(qtreeAllStmtTime).append(" ");
    	for(int i = 0; i < topK.length;i++){
    		sb.append(topKPerc[i]).append(" ");
    		sb.append(topKPrec[i]).append(" ");
    		sb.append(topKTimes[i]).append(" ");
    	}
    	sb.append(maxK).append("\n");
    	log.info(sb.toString(),true);
    	fis.write(sb.toString().getBytes());
    	
    	fis.flush();
    	
	}
        
    private List<Node> convertToNodeSet(
			Collection<String> relevantSourcesRanked) {
		List<Node> nodes = new ArrayList<Node>();
		for(String s: relevantSourcesRanked){
			nodes.add(new Resource(s));
		}
		return nodes;
	}

    private void dumpSources(String prefix, Collection<Node> srcs, File file) {
    	try{
    		FileOutputStream fos; 
    		if(file.exists())
    			fos = new FileOutputStream(file,true);
    		else
    			fos = new FileOutputStream(file);
    	
    	fos.write(("====== "+prefix+" =======\n").getBytes());
    	for(Node src: srcs){
    		fos.write((src.toString()+"\n").getBytes());
    	}
    	fos.flush();fos.close();
    	}catch(Exception e){
    		e.printStackTrace();
    	}
		
	}

	private int countStmts(List<Node[]> tmp) {
		Set<Nodes> res = new HashSet<Nodes>();
		for(Node [] n: tmp){
			res.add(new Nodes(Arrays.copyOfRange(n, 0, (n.length - (n.length/4)))));
		}
		return res.size();
	}

	private List<Node[]> computeJoin(Integer[] joinOrder, int[][] joins,
    		Node[][] bgps, Map<Node, List<Node[]>> srcMap, Collection<Node> relSources) {
    	
		List<Node[]> current = getResultTriples(bgps[joinOrder[0]], relSources ,srcMap);
    	
    	log.info("Join Order {}", Arrays.toString(joinOrder));
    	for(int i=1; i <joinOrder.length;i++){
        	List<Node[]> gpresults = getResultTriples(bgps[joinOrder[i]] , relSources, srcMap);
        	current = computeJoin(current, joins[i-1][0], gpresults, joins[i-1][1]);
        	log.info("Compute Join [{} stmts for {}]{} :{} X {}:{}[{} stmts for {}]",new Object[]{
					current.size(),
					Nodes.toN3(bgps[joinOrder[i-1]]),
					joinOrder[i-1], joins[i-1][0],
					joins[i-1][1],joinOrder[i],
					gpresults.size(),
					Nodes.toN3(bgps[i])
					});

        }
    	return current;
    } 
    
    private static double getTopKPrecision(List<Node> topKSrcs,
    	    Set<Node> countSources) {
    	double all = topKSrcs.size();
    	double matches = 0D;
    	for(Node cs : countSources){
    	    if(topKSrcs.contains(cs)) matches++;
    	}
    	return matches/all;
    }

    private static List<Node> getTopKSources(int k,
    	    List<Node> rankOrderedSources) {
    	List<Node> topKsrc = new ArrayList<Node>();
    	for(int i=0; i< Math.min(k, rankOrderedSources.size()); i++){
    	    topKsrc.add(rankOrderedSources.get(i));
    	}
    	return topKsrc;
    }

    private static int getMaxK(List<Node> rankOrderedSources,
    	    Set<Node> countSources) {
    	int currentPosition = 0;
    	for(Node s: rankOrderedSources){
    	    currentPosition++;
    	    if(countSources.contains(s)){
    	    	countSources.remove(s);
    	    } else{
    	    	if(countSources.isEmpty()) 
    	    		return currentPosition;   
    	    }
    	}
    	return currentPosition;
    }
    
    public List<Node[]>  computeJoin(List<Node[]> leftNodes, int lpos, List<Node[]> rightNodes, int rpos) {
    	List<Node[]> result = new ArrayList<Node[]>();
    	for (Node[] lnx : leftNodes) {
    	    Node ljc = lnx[lpos];
    	    for (Node[] rnx : rightNodes) {
    	    	Node rjc = rnx[rpos];
    	    	if (ljc.equals(rjc)) {
    	    		Node[] comb = new Node[lnx.length + rnx.length];
    	    		int lnxPref = lnx.length - (lnx.length/4);
    	    		for( int i =0; i <lnxPref; i++){
    	    			comb [i] = lnx[i];
    	    		}
    	    		for (int i=lnxPref; i< (lnxPref+3); i++){
    	    			comb [i] = rnx[i-lnxPref];
    	    		}
    	    		for( int i = lnxPref+3; i < lnx.length+3; i++){
    	    			comb [i] = lnx[i-3];
    	    		}
    	    		comb[comb.length-1] = rnx [3];
    	    		result.add(comb);
    	    	}
    	    }
    	}
    	leftNodes = null;
    	rightNodes = null;
    	return result;
    }
    
    private Set<Node> countSources(Collection<Node[]> current) {
    	Set<Node> resSet = new HashSet<Node>();
    	for(Node[] n: current){
    	    int i = n.length - (n.length/4);
    	    for( ; i < n.length;i++){
    	    	resSet.add(n[i]);
    	    }
    	}
    	return resSet;
    }
       
    boolean match(Node[] nx ,Node [] filter) {
		boolean tmp = true;
		
		for (int i = 0; i < filter.length; i++) {
			if (! (filter[i] instanceof Variable) ) {
				if (! filter[i].equals(nx[i])) {
					tmp = false;
				}
			}
		}
		return tmp;
	}
    
    private List<Node[]> getResultTriples(Node[] bgp, Collection<Node> srcs, Map<Node, List<Node[]>> srcMap) {
    	log.info("Filtering {} from {} srcs",new Object[]{Nodes.toN3(bgp),srcs.size()});
    	List<Node[]> resSet = new ArrayList<Node[]>();
    	for(Node src: srcs){
    	    if(srcMap.containsKey(src)){
    	    	List<Node[]> nodeList = srcMap.get(src);
    	    	for(Node[] nodes: nodeList){
    	    		if(match(nodes,bgp)){
    	    			resSet.add(new Node[]{nodes[0],nodes[1],nodes[2], src});
    	    		}
    	    	}
    	    }
    	}
    	return resSet;
    }
    
//	private void evaluate(QueryResultEstimation qre, int queryCount, Model model,  FileOutputStream fis,
//			File input) throws IOException {
//		long startEval = System.currentTimeMillis();
//		
//		log.info("Evaluating now Query "+queryCount);
//		log.info("Q["+queryCount+"] QueryResultEstimation\n"+qre);
//		int [] joinBuckets = qre.getJoinResultingBuckets();
//
//		int realStmt = evaluateRealStmts(model,qre.getQueryString());
//		log.info("Q["+queryCount+"] real_stmt: "+realStmt);
//		Model m = ModelFactory.createDefaultModel();
//		
//		Node [] [] filter = qp.transform(qre.getQueryString());
//		 
//		long start = System.currentTimeMillis();
//		int qtreeStmt = evaluateQTreeStmts(qre.getRelevantSourcesRanked(),qre.getQueryString(),m,0,Integer.MAX_VALUE,input,filter);
//		long qtreeEval = System.currentTimeMillis() - start;
//		log.info("Q["+queryCount+"] qtree_stmt: "+qtreeStmt+" ("+qtreeStmt/realStmt+") "+qtreeEval+" ms");
//		m=null;
//		m = ModelFactory.createDefaultModel();
//		start = System.currentTimeMillis();
//		int qtreeTop10Stmt = evaluateQTreeStmts(qre.getRelevantSourcesRanked(),qre.getQueryString(),m,0,10,input,filter);
//		long qtreeTop10Eval = System.currentTimeMillis() - start;
//		log.info("Q["+queryCount+"] qtreeTop10Stmt: "+qtreeTop10Stmt+" ("+qtreeTop10Stmt/realStmt+")"+qtreeTop10Eval+" ms");
//		start = System.currentTimeMillis();
//		int qtreeTop50Stmt = evaluateQTreeStmts(qre.getRelevantSourcesRanked(),qre.getQueryString(),m,10,50,input,filter);
//		long qtreeTop50Eval = System.currentTimeMillis() - start;
//		log.info("Q["+queryCount+"] qtreeTop50Stmt: "+qtreeTop50Stmt+" ("+qtreeTop50Stmt/realStmt+")"+qtreeTop50Eval+" ms");
//		
//		start = System.currentTimeMillis();
//		int qtreeTop100Stmt = evaluateQTreeStmts(qre.getRelevantSourcesRanked(),qre.getQueryString(),m,50,100,input,filter);
//		long qtreeTop100Eval = System.currentTimeMillis() - start;
//		log.info("Q["+queryCount+"] qtreeTop100Stmt: "+qtreeTop100Stmt+" ("+qtreeTop100Stmt/realStmt+")"+qtreeTop100Eval+" ms");
//		
//		start = System.currentTimeMillis();
//		int qtreeTop200Stmt = evaluateQTreeStmts(qre.getRelevantSourcesRanked(),qre.getQueryString(),m,100,200,input,filter);
//		long qtreeTop200Eval = System.currentTimeMillis() - start;
//		log.info("Q["+queryCount+"] qtreeTop200Stmt: "+qtreeTop200Stmt+" ("+qtreeTop200Stmt/realStmt+")"+qtreeTop200Eval+" ms");
//		
//		m =null;
//		long joinEval = 0L;
//		if(joinBuckets.length!=0)
//		  joinEval = qre.getJoinEvalTimes()[joinBuckets.length-1];
//			
//		int joinBucket = 0;
//		if(joinBuckets.length!=0)
//			joinBucket = joinBuckets[joinBuckets.length-1];
//		else if(qre.getBgpResultingBuckets()!=null && qre.getBgpResultingBuckets().length==1){
//			joinBucket = qre.getBgpResultingBuckets()[0];
//		}
//		StringBuilder sb = new StringBuilder();
//		log.info("Q["+queryCount+"] total_time: "+qre.getTotalQueryTime());
//		sb.append(queryCount).append(" ").append(qre.getTotalQueryTime()).append(" ").append(joinEval).append(" ").append(qre.getRankTime());
//		sb.append(" ").append(joinBucket);
//		if(qre.getRelevantSourcesRanked()==null||qre.getRelevantSourcesRanked().size()==0)
//			sb.append(" 0 ");
//		else
//			sb.append(" ").append(qre.getRelevantSourcesRanked().size()).append(" ");
//		log.info("Q["+queryCount+"] res_buckets: "+joinBucket);
//		log.info("Q["+queryCount+"] res_sources: "+qre.getRelevantSourcesRanked().size());
//		if(realStmt == -1) {sb.append("0");
//			qtreeTop10Stmt=0;
//			qtreeTop50Stmt=0;
//			qtreeTop100Stmt=0;
//			qtreeTop200Stmt=0;
//			qtreeStmt=0;
//		}
//		else {sb.append(realStmt);}
//		
//		sb.append(" ").append(qtreeStmt/(double)realStmt).append(" ")
//		.append(qtreeTop10Stmt/(double)realStmt).append(" ")
//		.append(qtreeTop50Stmt/(double)realStmt).append(" ")
//		.append(qtreeTop100Stmt/(double)realStmt).append(" ")
//		.append(qtreeTop200Stmt/(double)realStmt).append(" ")
//		.append(qtreeEval).append(" ").append(qtreeTop10Eval).append(" ").append(qtreeTop50Eval).append(" ").append(qtreeTop100Eval).append(" ").append(qtreeTop200Eval)
//		.append("\n");
//		fis.write(sb.toString().getBytes());
//		fis.flush();
//		
//		qre.setIndexEval(sb.toString());
//		
//		log.info("Q["+queryCount+"] Result: "+sb.toString()+" "+qre.getTotalQueryTime()+" (ms), evalTime: "+(System.currentTimeMillis()-startEval) +" (ms)",true); 
//	}

//	private int evaluateQTreeStmts(ArrayList<String> sources,
//			String queryString, Model model, int start, int topK, File input,Node [][] filter) {
//		if(sources==null||sources.size()==0) return 0;	
//		com.hp.hpl.jena.query.Query query = QueryFactory.create(queryString) ;
//		
//		loadModel(input, sources.subList(0, Math.min(topK,sources.size())),model,filter);
//		QueryExecution qexec = QueryExecutionFactory.create(query, model) ;
//		int realCount = 0;
//		try {
//			ResultSet results = qexec.execSelect() ;
//			if(results.hasNext()) realCount=0;
//			for ( ; results.hasNext() ; )
//			{
//				results.nextSolution();
//				realCount++;
//			}
//		} finally { qexec.close() ;  }
//		return realCount;
//	}


//	private void loadModel(File input, List<String> sources, Model model, Node[][]filter) {
////		log.info("Number of sources: "+sources.size());
//		if(sources==null||sources.size()==0);
//		NxParser p;
//		try {
//			InputStream is = new FileInputStream(input);
//			if(input.getName().endsWith(".gz"))
//				is = new GZIPInputStream(is);
//			p = new NxParser(is);
//			while(p.hasNext()){
//				Node[]stmt = p.next();
//				if(sources.contains(stmt[3].toString()) && match(stmt,filter)){
//					Triple t = new Triple(transformToNode(stmt[0]), transformToNode(stmt[1]),transformToNode(stmt[2]));
//					model.add(model.asStatement(t));
//				}
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}

	boolean match(Node[] nx ,Node [][] filter) {
		for(Node [] bgp: filter){
			boolean tmp = true;
			for (int i = 0; i < bgp.length; i++) {
				if (! (bgp[i] instanceof Variable) ) {
					if (! bgp[i].equals(nx[i])) {
						tmp = false;
					}
				}
			}
			if(tmp) return true;
		}
		return false;
	}
	


	public com.hp.hpl.jena.graph.Node transformToNode(Node n ){
		if (n instanceof Resource) {
			return com.hp.hpl.jena.graph.Node.createURI(((Resource)n).toString());
		} else if (n instanceof Literal) {
			return com.hp.hpl.jena.graph.Node.createLiteral(((Literal)n).toString());
		} else if (n instanceof BNode) {
			return com.hp.hpl.jena.graph.Node.createAnon(new AnonId(((BNode)n).toString()));
		}
		return null; 
	}


	private int evaluateRealStmts(Model allModel, String queryString) {
		com.hp.hpl.jena.query.Query query = QueryFactory.create(queryString) ;
		QueryParser qp = new QueryParser();
		
		QueryExecution qexec = QueryExecutionFactory.create(query, allModel) ;
		int realCount = -1;
		try {
			ResultSet results = qexec.execSelect() ;
			if(results.hasNext()) realCount=0;
			for ( ; results.hasNext() ; )
			{
				QuerySolution soln = results.nextSolution();
				realCount++;
			}
		} finally { qexec.close() ; }
		return realCount;

	}


@Override
public String getDescription() {
	return "benchmark hashing function";
}
}
