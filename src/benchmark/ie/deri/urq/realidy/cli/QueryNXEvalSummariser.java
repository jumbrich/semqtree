package ie.deri.urq.realidy.cli;

import ie.deri.urq.realidy.bench.utils.NXBenchSummary;

import java.util.Hashtable;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryNXEvalSummariser extends CLIObject{
	private static final Logger log = LoggerFactory.getLogger(QueryNXEvalSummariser.class);
	private Hashtable<String, List<NXBenchSummary>> queryMap = new Hashtable<String, List<NXBenchSummary>>();
	private Hashtable<String, List<NXBenchSummary>> setupMap = new Hashtable<String, List<NXBenchSummary>>();

	@Override
	protected void addOptions(Options opts) {
		opts.addOption(CLIObject.OPTION_GENERIC_INPUT);
		opts.addOption(CLIObject.OPTION_GENERIC_OUTPUT);
	}

	
	@Override
	protected void execute(CommandLine cmd) {
//		// TODO Auto-generated method stub
//		File queryRoot = new File(cmd.getOptionValue(CLIObject.PARAM_GENERIC_INPUT));
//		File plotRoot = new File(cmd.getOptionValue(CLIObject.PARAM_GENERIC_OUTPUT));
//
//		File [] queryFiles = queryRoot.listFiles();
//		for(File queryFile: queryFiles){
//			if(queryFile.isHidden() || queryFile.getName().endsWith(".zip")) continue;
//			log.info("Processing {}",queryFile);
//			File[] querybenchResults = new File(queryFile,"nxEval").listFiles();
//			for(File queryBench: querybenchResults){
//				if(queryBench.isDirectory() || queryBench.getName().endsWith(".srcs") || queryBench.getName().endsWith(".zip")) continue;
//				try{
//				//summarise the results and save them in a map
//					log.info("  Processing {}",queryBench);
//					NXBenchSummary s = new NXBenchSummary();
//					s.process(queryBench);
//					perQuery(s);
//					perSetup(s);
//				}catch(Exception e){
//					log.info("{} for file {} msg{}",new Object[]{e.getClass().getSimpleName(),queryFile.getAbsolutePath(),e.getMessage()});
//				}
//			}
//		}
//		
//		printSetup(setupMap,queryRoot,plotRoot);
//		printQuery(queryMap,queryRoot,plotRoot);
	}

//	private void printSetup(Hashtable<String, List<NXBenchSummary>> map, File queryRoot, File plotRoot) {
//		for(String s: map.keySet()){
//			log.info("=====SETUP: {} size {}=====", new Object[]{s,map.get(s).size()});
//			int counter =0;
//			for(NXBenchSummary b: map.get(s)){
//				for(Entry<Integer, String> ent: NXBenchSummary.labels.entrySet()){
//					try {
//						File f = new File(plotRoot,s+"-"+ent.getValue()+".stats");
//						FileWriter fw;
//						if(f.exists())
//							fw = new FileWriter(f,true);
//						else{
//							fw = new FileWriter(f);
//						}
//						fw.write((counter+" "+b.queryString(ent.getKey())+"\n"));
//						fw.flush();
//						fw.close();
//					} catch (IOException e) {
//						e.printStackTrace();
//					}
//				}
//				counter++;
//			}
//		}
//		for(String index: setupMap.keySet()){
//			for(Entry<Integer, String> ent: NXBenchSummary.labels.entrySet()){
//				try {
//				
//					Printer.printNXEval(plotRoot, new File(plotRoot, index+"-"+ent.getValue()+".stats"), ent.getValue());
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
//			}
//		}
//		
//	}
//	
//	private void printQuery(Hashtable<String, List<NXBenchSummary>> map, File queryRoot, File plotRoot) {
//		for(String s: map.keySet()){
//			log.info("=====QUERY: {} size {}=====", new Object[]{s,map.get(s).size()});
//			int counter =0;
//			for(NXBenchSummary b: map.get(s)){
//				for(Entry<Integer, String> ent: NXBenchSummary.labels.entrySet()){
//					try {
//						File f = new File(plotRoot,s+"-"+ent.getValue()+".stats");
//						FileWriter fw;
//						if(f.exists())
//							fw = new FileWriter(f,true);
//						else{
//							fw = new FileWriter(f);
//						}
//						fw.write((counter+" "+b.indexString(ent.getKey())+"\n"));
//						fw.flush();
//						fw.close();
//					} catch (IOException e) {
//						e.printStackTrace();
//					}
//				}
//				counter++;
//			}
//		}
//		for(String index: queryMap.keySet()){
//			for(Entry<Integer, String> ent: NXBenchSummary.labels.entrySet()){
//				try {
//					Printer.printNXEval(plotRoot, new File(plotRoot, index+"-"+ent.getValue()+".stats"), ent.getValue());
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
//			}
//		}
//		
//	}
//
//
//	private void perSetup(NXBenchSummary s) {
//		List<NXBenchSummary> bl = setupMap.get(s.getIndex());
//		if(bl == null ) bl = new ArrayList<NXBenchSummary>();
//		bl.add(s);
//		setupMap.put(s.getIndex(),bl);
//	}
//
//
//	private void perQuery(NXBenchSummary s) {
//		List<NXBenchSummary> bl = queryMap.get(s.getQuery());
//		if(bl == null ) bl = new ArrayList<NXBenchSummary>();
//		bl.add(s);
//		queryMap.put(s.getQuery(),bl);
//	}


	@Override
	public String getDescription() {
		return "Summaries the results from a query folder";
	}
}