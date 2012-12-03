package ie.deri.urq.realidy.cli;

import ie.deri.urq.realidy.bench.utils.Printer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.parser.NxParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class EvaluateQueries extends CLIObject{

	private final static Logger log = LoggerFactory.getLogger(EvaluateQueries.class);

	private static List<String> getTopKSources(int k,
			ArrayList<String> rankOrderedSources) {
		List<String> topKsrc = new ArrayList<String>();
		for(int i=0; i<Math.min(k, rankOrderedSources.size()); i++){
			topKsrc.add(rankOrderedSources.get(i));
		}
		return topKsrc;
	}

	private static QueryResultEstimation deserialise(File serQueryFile) throws IOException, ClassNotFoundException {
		log.info("Deserialise "+serQueryFile);
		FileInputStream fis = new FileInputStream(serQueryFile);
		ObjectInputStream ois = new ObjectInputStream(fis);
		Object o = ois.readObject();
		ois.close();
		fis.close();

		return (QueryResultEstimation) o;
	}

	@Override
	protected void addOptions(Options opts) {
		opts.addOption(CLIObject.OPTION_QUERY_FILE);
		opts.addOption(CLIObject.OPTION_OUTPUT_DIR);
		opts.addOption(CLIObject.OPTION_LOCAL_INSERT);
	}

	@Override
	protected void execute(CommandLine cmd) {
		File queryFolder = new File(cmd.getOptionValue(CLIObject.PARAM_QUERY_FILE));
		File outputDir = new File(cmd.getOptionValue(CLIObject.PARAM_OUTPUT_DIR));
		outputDir.mkdirs();
		File input = new File(cmd.getOptionValue(CLIObject.PARAM_LOCAL_INSERT));
		
//		benchmarkQueries(queryFolder,outputDir,input,"",new File("") , new File(""));
		
	}

	public void benchmarkQueries(File queryFolder, File outFile, File input,String hashing, File queryFile, File plotDir, String versionType) {
//		evalDir.mkdirs();
		outFile.getParentFile().mkdirs();
		queryFolder.mkdirs();
		log.info("create Jena evaluation model");
		long start = System.currentTimeMillis();
		Model model = createEvalModel(input);
		log.info("Time {} ms",System.currentTimeMillis()-start);
		
		try {
//			
			FileOutputStream fis = new FileOutputStream(outFile);
			fis.write(("#queryNo totaltime jointime ranktime buckets sources realStmt qtreeAllStmt top10Stmt top50Stmt top100Stmt top200Stmt\n").getBytes());

			File [] queries = queryFolder.listFiles();

			for(File f: queries){
				try {
					if(f.getName().startsWith("query_"+versionType+"-"+hashing+"_"+queryFile.getName()+".qre.")){
						String name = f.getName();
						String numeric = name.substring(name.lastIndexOf("qre.")+4,name.lastIndexOf(".ser"));
						int queryCount =  Integer.valueOf(numeric);
						QueryResultEstimation qre = deserialise(f);
//						log.info("Result for Query "+queryFile.getName()+"-"+queryCount+" \n{}",qre);
						
						evaluate(qre,queryCount,model,fis,input);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			fis.flush();fis.close();
			Printer.printQueryPlots(outFile,plotDir);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private Model createEvalModel(File input) {
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
		return m;
	}

	private void evaluate(QueryResultEstimation qre, int queryCount, Model model,  FileOutputStream fis,
			File input) throws IOException {
		log.info("Evaluating now Query {} \n{}",new Object[]{queryCount,qre.toString()});
		int [] joinBuckets = qre.getJoinResultingBuckets();

		int realStmt = evaluateRealStmts(model,qre.getQueryString());
		log.info("Query {} real stmt: {}",new Object[]{queryCount,realStmt});
		int qtreeStmt = evaluateQTreeStmts(qre.getRelevantSourcesRanked(),qre.getQueryString(),Integer.MAX_VALUE,input);
		log.info("Query {} qtreeStmt: {}",new Object[]{queryCount,qtreeStmt});
		int qtreeTop10Stmt = evaluateQTreeStmts(qre.getRelevantSourcesRanked(),qre.getQueryString(),10,input);
		log.info("Query {} qtreeTop10Stmt: {}",new Object[]{queryCount,qtreeTop10Stmt});
		int qtreeTop50Stmt = evaluateQTreeStmts(qre.getRelevantSourcesRanked(),qre.getQueryString(),50,input);
		log.info("Query {} qtreeTop50Stmt: {}",new Object[]{queryCount,qtreeTop50Stmt});
		int qtreeTop100Stmt = evaluateQTreeStmts(qre.getRelevantSourcesRanked(),qre.getQueryString(),100,input);
		log.info("Query {} qtreeTop100Stmt: {}",new Object[]{queryCount,qtreeTop100Stmt});
		int qtreeTop200Stmt = evaluateQTreeStmts(qre.getRelevantSourcesRanked(),qre.getQueryString(),200,input);
		log.info("Query {} qtreeTop200Stmt: {}",new Object[]{queryCount,qtreeTop200Stmt});
		long joinEval = 0L;
		if(joinBuckets.length!=0)
		  joinEval = qre.getJoinEvalTimes()[joinBuckets.length-1];
		int joinBucket = 0;
		if(joinBuckets.length!=0)
			joinBucket = joinBuckets[joinBuckets.length-1];
		StringBuilder sb = new StringBuilder();
		sb.append(queryCount).append(" ").append(qre.getTotalQueryTime()).append(" ").append(joinEval).append(" ").append(qre.getRankTime());
		sb.append(" ").append(joinBucket);
		if(qre.getRelevantSourcesRanked()==null||qre.getRelevantSourcesRanked().size()==0)
			sb.append(" 0 ");
		else
			sb.append(" ").append(qre.getRelevantSourcesRanked().size()).append(" ");
		
		if(realStmt == -1) {sb.append("0");
			qtreeTop10Stmt=0;
			qtreeTop50Stmt=0;
			qtreeTop100Stmt=0;
			qtreeTop200Stmt=0;
			qtreeStmt=0;
		}
		else {sb.append(realStmt);}
		
		sb.append(" ").append(Math.abs((double)qtreeStmt/(double)realStmt)).append(" ")
		.append(Math.abs(qtreeTop10Stmt/(double)realStmt)).append(" ")
		.append(Math.abs(qtreeTop50Stmt/(double)realStmt)).append(" ")
		.append(Math.abs(qtreeTop100Stmt/(double)realStmt)).append(" ")
		.append(Math.abs(qtreeTop200Stmt/(double)realStmt)).append("\n");
		fis.write(sb.toString().getBytes());
		fis.flush();
		
		log.info("Result: {}",sb.toString()); 
	}


	private int evaluateQTreeStmts(ArrayList<String> sources,
			String queryString, int topK, File input) {
		if(sources==null||sources.size()==0) return 0;
		Model m = loadModel(input, sources.subList(0, Math.min(topK,sources.size())));

		com.hp.hpl.jena.query.Query query = QueryFactory.create(queryString) ;
		QueryExecution qexec = QueryExecutionFactory.create(query, m) ;
		int realCount = -1;
		try {
			ResultSet results = qexec.execSelect() ;
			if(results.hasNext()) realCount=0;
			for ( ; results.hasNext() ; )
			{
				QuerySolution soln = results.nextSolution();
				realCount++;
			}
		} finally { qexec.close() ;  m = null; }
		return realCount;
	}


	private Model loadModel(File input, List<String> sources) {
//		log.info("Number of sources: "+sources.size());
		
		Model m = ModelFactory.createDefaultModel();
		if(sources==null||sources.size()==0) return m;
		NxParser p;
		try {
			InputStream is = new FileInputStream(input);
			if(input.getName().endsWith(".gz"))
				is = new GZIPInputStream(is);
			p = new NxParser(is);
			while(p.hasNext()){
				Node[]stmt = p.next();
				if(sources.contains(stmt[3].toString())){
					Triple t = new Triple(transformToNode(stmt[0]), transformToNode(stmt[1]),transformToNode(stmt[2]));
					m.add(m.asStatement(t));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return m;
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
		return "evaluate queryies";
	}
}