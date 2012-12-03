package ie.deri.urq.realidy.cli;

import ie.deri.urq.realidy.bench.utils.JoinOrderBenchSummary;
import ie.deri.urq.realidy.bench.utils.Printer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryOperatorEvaluation extends CLIObject{
	private static final Logger log = LoggerFactory.getLogger(QueryOperatorEvaluation.class);
	private Map<String, Map<String,JoinOrderBenchSummary>> benchMap = new HashMap<String, Map<String,JoinOrderBenchSummary>>();
	private Map<String, Map<String,JoinOrderBenchSummary>> benchMapNoOpt = new HashMap<String, Map<String,JoinOrderBenchSummary>>();
	private Map<String, Map<String,JoinOrderBenchSummary>> queryMap = new HashMap<String, Map<String,JoinOrderBenchSummary>>();
	private Map<String, Map<String,JoinOrderBenchSummary>> queryMapNoOpt = new HashMap<String, Map<String,JoinOrderBenchSummary>>();
	private Map<String, Map<String,JoinOrderBenchSummary>> setupMap = new  HashMap<String, Map<String,JoinOrderBenchSummary>>();
	private Map<String, Map<String,JoinOrderBenchSummary>> setupMapNoOpt =new  HashMap<String, Map<String,JoinOrderBenchSummary>>();

	@Override
	protected void addOptions(Options opts) {
		opts.addOption(CLIObject.OPTION_GENERIC_INPUT);
		opts.addOption(CLIObject.OPTION_GENERIC_OUTPUT);
	}

	public static void main(String[] args) {
		QueryOperatorEvaluation q = new QueryOperatorEvaluation();
		q.run(new String[]{"-i","evaluation/vldb-people/b10K_m100K_true/queries","-o",""});
	}

	@Override
	protected void execute(CommandLine cmd) {
		File queryRoot = new File(cmd.getOptionValue(CLIObject.PARAM_GENERIC_INPUT));

		File plotRoot = new File(cmd.getOptionValue(CLIObject.PARAM_GENERIC_OUTPUT));

		File [] queryFiles = queryRoot.listFiles();
		for(File queryFile: queryFiles){
			File[] querybenchResults = queryFile.listFiles();
			for(File queryBench: querybenchResults){
				if(queryBench.isDirectory() || queryBench.getName().endsWith(".srcs")) continue;
				try{
					JoinOrderBenchSummary s = getBenchmark(queryBench);
					s.process(queryBench);
					perQuery(s);
					perSetup(s);
				}catch(Exception e){
					log.info("{} for file {} msg{}",new Object[]{e.getClass().getSimpleName(),queryBench.getAbsolutePath(),e.getMessage()});
				}
			}
		}

		printSetup(queryRoot,plotRoot);
		printQuery(queryRoot,plotRoot);
	}

	private JoinOrderBenchSummary getBenchmark(File queryBench) {
		JoinOrderBenchSummary  b = new JoinOrderBenchSummary(queryBench);
		JoinOrderBenchSummary  b1;

		Map<String, Map<String,JoinOrderBenchSummary>> map = benchMap;
		if(b.noOpt())
			map = benchMapNoOpt;

		Map<String, JoinOrderBenchSummary> bl = map.get(b.getIndex());
		if(bl == null ) {
			bl = new HashMap<String,JoinOrderBenchSummary>();
			map.put(b.getIndex(),bl);
		}
		b1 = map.get(b.getIndex()).get(b.getQuery());

		if(b1 == null) b1 = b;
		map.get(b.getIndex()).put(b.getQuery(), b1);

		return b1;
	}


	private void printSetup(File queryRoot, File plotRoot) {
		for(String s: setupMap.keySet()){
			System.out.println("=====SETUP: "+s+" =====");
			int count = 0;
			for(String index: setupMap.get(s).keySet()){
				JoinOrderBenchSummary b =null,b1=null;
				if(setupMap.get(s) != null && setupMap.get(s).get(index) != null)
					b = setupMap.get(s).get(index);
				if(setupMapNoOpt.get(s) != null && setupMapNoOpt.get(s).get(index) != null)
					b1 = setupMapNoOpt.get(s).get(index);

				if(b!=null && b1!=null){
					for(Entry<Integer, String> ent: JoinOrderBenchSummary.labels.entrySet()){
						try {
							File f = new File(plotRoot,s+"-"+ent.getValue()+".joinorder.stats");
							FileWriter fw;
							if(f.exists())
								fw = new FileWriter(f,true);
							else{
								fw = new FileWriter(f);
							}
							fw.write((count+" " +b.queryString(ent.getKey())+" noOpt: "+ b1.queryString(ent.getKey())+"\n"));
							fw.flush();
							fw.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
					count++;
				}
			}
		}
		for(String index: setupMap.keySet()){
			for(Entry<Integer, String> ent: JoinOrderBenchSummary.labels.entrySet()){
				try {
					Printer.printJoinOrderEval(plotRoot, new File(plotRoot, index+"-"+ent.getValue()+".joinorder.stats"), ent.getValue());
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}

	private void printQuery(File queryRoot, File plotRoot) {

		for(String query: queryMap.keySet()){
			System.out.println("=====QUERY: "+query+" =====");
			int count = 0;
			for(String index: queryMap.get(query).keySet()){
				JoinOrderBenchSummary b = queryMap.get(query).get(index);
				JoinOrderBenchSummary b1 = queryMapNoOpt.get(query).get(index);
				for(Entry<Integer, String> ent: JoinOrderBenchSummary.labels.entrySet()){
					try {
						File f = new File(plotRoot, query+"-"+ent.getValue()+".joinorder.stats");
						FileWriter fw;
						if(f.exists())
							fw = new FileWriter(f,true);
						else{
							fw = new FileWriter(f);
						}
						fw.write((count+" "+b.indexString(ent.getKey())+" noOpt: "+ b1.indexString(ent.getKey())+"\n"));
						fw.flush();
						fw.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				count++;
			}
		}

		for(String query: queryMap.keySet()){
			for(Entry<Integer, String> ent: JoinOrderBenchSummary.labels.entrySet()){
				try {
					Printer.printJoinOrderEval(plotRoot, new File(plotRoot, query+"-"+ent.getValue()+".joinorder.stats"),ent.getValue());
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}


	private void perSetup(JoinOrderBenchSummary s) {
		Map<String, Map<String,JoinOrderBenchSummary>> map = setupMap;
		if(s.noOpt())
			map = setupMapNoOpt;

		Map<String, JoinOrderBenchSummary> bl = map.get(s.getIndex());
		if(bl == null ) bl = new HashMap<String,JoinOrderBenchSummary>();

		bl.put(s.getQuery(),s);
		map.put(s.getIndex(),bl);
	}


	private void perQuery(JoinOrderBenchSummary s) {
		Map<String, Map<String,JoinOrderBenchSummary>> map = queryMap;
		if(s.noOpt())
			map = queryMapNoOpt;

		Map<String,JoinOrderBenchSummary> bl = map.get(s.getQuery());
		if(bl == null ) bl = new HashMap<String,JoinOrderBenchSummary>();
		bl.put(s.getIndex(),s);
		map.put(s.getQuery(),bl);

	}


	@Override
	public String getDescription() {
		return "Summaries the results from a query folder";
	}
}