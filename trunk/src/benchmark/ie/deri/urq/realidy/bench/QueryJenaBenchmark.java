package ie.deri.urq.realidy.bench;

import ie.deri.urq.realidy.bench.utils.BenchmarkConfig;
import ie.deri.urq.realidy.bench.utils.Printer;
import ie.deri.urq.realidy.hashing.QTreeHashing;
import ie.deri.urq.realidy.index.SemQTree;
import ie.deri.urq.realidy.query.arq.QueryParser;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.Variable;
import org.semanticweb.yars.nx.parser.NxParser;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.AnonId;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import de.ilmenau.datasum.index.QueryResultEstimation;

public class QueryJenaBenchmark extends Benchmark {
	private final QueryParser qp = new QueryParser();
	private SemQTree sqt;

	public QueryJenaBenchmark(BenchmarkConfig config, String version,
			QTreeHashing hashing) {
		super(config,version ,hashing);
		sqt = SemQTree.loadIndex(config.getIndexFile(version,hashing.getHasherName()));
		
	}

	
	public QueryJenaBenchmark(SemQTree sqt, File query, File inputData,
			File outRoot) {
		super(new BenchmarkConfig(), sqt.getVersionType(),sqt.getHasher());
		this.sqt = sqt;
		
		getConfig().setRootDir(outRoot);
		getConfig().setInputData(inputData);
		getConfig().setQueryFile(query);
		getConfig().init();
	}


	@Override
	public void benchmark() {
		getConfig().jenaEvalRoot().mkdirs();
		long start = System.currentTimeMillis();
		Model model = createEvalModel(getConfig().inputData());
		log("Time "+(System.currentTimeMillis()-start)+" ms");
		
		try {
			FileOutputStream fis = new FileOutputStream(getConfig().jenaEvalQueryFile(_version,_hasher));
			fis.write(("#queryNo totaltime jointime ranktime buckets sources realStmt qtreeAllStmt top10Stmt top50Stmt top100Stmt top200Stmt qtreeTime top10Time top50Time top100Time top200Time\n").getBytes());

			File [] queries = getConfig().queryRoot().listFiles();
			for(File f: queries){
				if(!f.isFile())continue;
				try {
						String name = f.getName();
						String numeric = name.substring(name.lastIndexOf("qre.")+4,name.lastIndexOf(".ser"));
						int queryCount =  Integer.valueOf(numeric);
						QueryResultEstimation qre = deserialise(f);
						evaluate(qre, queryCount, model, fis, getConfig().inputData());
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			fis.flush();fis.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private  QueryResultEstimation deserialise(File serQueryFile) throws IOException, ClassNotFoundException {
		log("Deserialise "+serQueryFile);
		FileInputStream fis = new FileInputStream(serQueryFile);
		ObjectInputStream ois = new ObjectInputStream(fis);
		Object o = ois.readObject();
		ois.close();
		fis.close();

		return (QueryResultEstimation) o;
	}

	
	private Model createEvalModel(File input) {
		long start = System.currentTimeMillis();
		Model m = ModelFactory.createDefaultModel();
		NxParser p;
		try {
			InputStream is = new FileInputStream(input);
			if(input.getName().endsWith(".gz"))
				is = new GZIPInputStream(is);
			p = new NxParser(is);
//			p = new NxParser(new GZIPInputStream(new FileInputStream(input)));
			while(p.hasNext()){
				Node[]stmt = p.next();
				Triple t = new Triple(transformToNode(stmt[0]), transformToNode(stmt[1]),transformToNode(stmt[2]));
				m.add(m.asStatement(t));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		log("Created model in "+(System.currentTimeMillis()-start)+" ms!");
		return m;
	}

	
	
	private void evaluate(QueryResultEstimation qre, int queryCount, Model model,  FileOutputStream fis,
			File input) throws IOException {
		long startEval = System.currentTimeMillis();
		
		log("Evaluating now Query "+queryCount);
		log("Q["+queryCount+"] QueryResultEstimation\n"+qre);
		int [] joinBuckets = qre.getJoinResultingBuckets();

		int realStmt = evaluateRealStmts(model,qre.getQueryString());
		log("Q["+queryCount+"] real_stmt: "+realStmt);
		Model m = ModelFactory.createDefaultModel();
		
		Node [] [] filter = qp.transform(qre.getQueryString());
		 
		long start = System.currentTimeMillis();
		int qtreeStmt = evaluateQTreeStmts(qre.getRelevantSourcesRanked(),qre.getQueryString(),m,0,Integer.MAX_VALUE,input,filter);
		long qtreeEval = System.currentTimeMillis() - start;
		log("Q["+queryCount+"] qtree_stmt: "+qtreeStmt+" ("+qtreeStmt/realStmt+") "+qtreeEval+" ms");
		m=null;
		m = ModelFactory.createDefaultModel();
		start = System.currentTimeMillis();
		int qtreeTop10Stmt = evaluateQTreeStmts(qre.getRelevantSourcesRanked(),qre.getQueryString(),m,0,10,input,filter);
		long qtreeTop10Eval = System.currentTimeMillis() - start;
		log("Q["+queryCount+"] qtreeTop10Stmt: "+qtreeTop10Stmt+" ("+qtreeTop10Stmt/realStmt+")"+qtreeTop10Eval+" ms");
		start = System.currentTimeMillis();
		int qtreeTop50Stmt = evaluateQTreeStmts(qre.getRelevantSourcesRanked(),qre.getQueryString(),m,10,50,input,filter);
		long qtreeTop50Eval = System.currentTimeMillis() - start;
		log("Q["+queryCount+"] qtreeTop50Stmt: "+qtreeTop50Stmt+" ("+qtreeTop50Stmt/realStmt+")"+qtreeTop50Eval+" ms");
		
		start = System.currentTimeMillis();
		int qtreeTop100Stmt = evaluateQTreeStmts(qre.getRelevantSourcesRanked(),qre.getQueryString(),m,50,100,input,filter);
		long qtreeTop100Eval = System.currentTimeMillis() - start;
		log("Q["+queryCount+"] qtreeTop100Stmt: "+qtreeTop100Stmt+" ("+qtreeTop100Stmt/realStmt+")"+qtreeTop100Eval+" ms");
		
		start = System.currentTimeMillis();
		int qtreeTop200Stmt = evaluateQTreeStmts(qre.getRelevantSourcesRanked(),qre.getQueryString(),m,100,200,input,filter);
		long qtreeTop200Eval = System.currentTimeMillis() - start;
		log("Q["+queryCount+"] qtreeTop200Stmt: "+qtreeTop200Stmt+" ("+qtreeTop200Stmt/realStmt+")"+qtreeTop200Eval+" ms");
		
		m =null;
		long joinEval = 0L;
		if(joinBuckets.length!=0)
		  joinEval = qre.getJoinEvalTimes()[joinBuckets.length-1];
			
		int joinBucket = 0;
		if(joinBuckets.length!=0)
			joinBucket = joinBuckets[joinBuckets.length-1];
		else if(qre.getBgpResultingBuckets()!=null && qre.getBgpResultingBuckets().length==1){
			joinBucket = qre.getBgpResultingBuckets()[0];
		}
		StringBuilder sb = new StringBuilder();
		log("Q["+queryCount+"] total_time: "+qre.getTotalQueryTime());
		sb.append(queryCount).append(" ").append(qre.getTotalQueryTime()).append(" ").append(joinEval).append(" ").append(qre.getRankTime());
		sb.append(" ").append(joinBucket);
		if(qre.getRelevantSourcesRanked()==null||qre.getRelevantSourcesRanked().size()==0)
			sb.append(" 0 ");
		else
			sb.append(" ").append(qre.getRelevantSourcesRanked().size()).append(" ");
		log("Q["+queryCount+"] res_buckets: "+joinBucket);
		log("Q["+queryCount+"] res_sources: "+qre.getRelevantSourcesRanked().size());
		if(realStmt == -1) {sb.append("0");
			qtreeTop10Stmt=0;
			qtreeTop50Stmt=0;
			qtreeTop100Stmt=0;
			qtreeTop200Stmt=0;
			qtreeStmt=0;
		}
		else {sb.append(realStmt);}
		
		sb.append(" ").append(qtreeStmt/(double)realStmt).append(" ")
		.append(qtreeTop10Stmt/(double)realStmt).append(" ")
		.append(qtreeTop50Stmt/(double)realStmt).append(" ")
		.append(qtreeTop100Stmt/(double)realStmt).append(" ")
		.append(qtreeTop200Stmt/(double)realStmt).append(" ")
		.append(qtreeEval).append(" ").append(qtreeTop10Eval).append(" ").append(qtreeTop50Eval).append(" ").append(qtreeTop100Eval).append(" ").append(qtreeTop200Eval)
		.append("\n");
		fis.write(sb.toString().getBytes());
		fis.flush();
		
		qre.setIndexEval(sb.toString());
		
		log("Q["+queryCount+"] Result: "+sb.toString()+" "+qre.getTotalQueryTime()+" (ms), evalTime: "+(System.currentTimeMillis()-startEval) +" (ms)",true); 
	}

	private int evaluateQTreeStmts(ArrayList<String> sources,
			String queryString, Model model, int start, int topK, File input,Node [][] filter) {
		if(sources==null||sources.size()==0) return 0;	
		com.hp.hpl.jena.query.Query query = QueryFactory.create(queryString) ;
		
		loadModel(input, sources.subList(0, Math.min(topK,sources.size())),model,filter);
		QueryExecution qexec = QueryExecutionFactory.create(query, model) ;
		int realCount = 0;
		try {
			ResultSet results = qexec.execSelect() ;
			if(results.hasNext()) realCount=0;
			for ( ; results.hasNext() ; )
			{
				results.nextSolution();
				realCount++;
			}
		} finally { qexec.close() ;  }
		return realCount;
	}


	private void loadModel(File input, List<String> sources, Model model, Node[][]filter) {
//		log.info("Number of sources: "+sources.size());
		if(sources==null||sources.size()==0);
		NxParser p;
		try {
			InputStream is = new FileInputStream(input);
			if(input.getName().endsWith(".gz"))
				is = new GZIPInputStream(is);
			p = new NxParser(is);
			while(p.hasNext()){
				Node[]stmt = p.next();
				if(sources.contains(stmt[3].toString()) && match(stmt,filter)){
					Triple t = new Triple(transformToNode(stmt[0]), transformToNode(stmt[1]),transformToNode(stmt[2]));
					model.add(model.asStatement(t));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

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
	public void benchmarkPlot() {
		try {
			Printer.printQueryPlots(getConfig().jenaEvalQueryFile(_version,_hasher), getConfig().plotRoot());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean exists() {
		// TODO Auto-generated method stub
		return false;
	}
	
	private void serialiseQueryResultEstimation(QueryResultEstimation qre,
			File file) throws IOException {
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file));
		oos.writeObject(qre);
		oos.close();
	}
	
}
