package ie.deri.urq.realidy.cli;

import ie.deri.urq.realidy.bench.utils.QueryResultSummary;

import java.io.File;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AllQueryEvaluation extends CLIObject{
	private final static Logger log = LoggerFactory.getLogger(AllQueryEvaluation.class);



	public static void main(String[] args) {
		AllQueryEvaluation e = new AllQueryEvaluation();
		e.run(new String[]{"-i","/Users/juum/Tmp/evalDumps/all/queries", "-o","/Users/juum/Tmp/evalDumps/all/eval"});
	}

	@Override
	protected void addOptions(Options opts) {
		//each benchmark needs an output file
		opts.addOption(CLIObject.OPTION_GENERIC_OUTPUT);
		opts.addOption(CLIObject.OPTION_GENERIC_INPUT);
	}
	@Override
	protected void execute(CommandLine cmd) {
		try {
			File outDir = new File(cmd.getOptionValue(CLIObject.PARAM_GENERIC_OUTPUT));
			if(!outDir.exists()) outDir.mkdirs();
			File inDir = new File(cmd.getOptionValue(CLIObject.PARAM_GENERIC_INPUT));

			File [] queryTypes = inDir.listFiles();
			
			for(File f: queryTypes){
				
				
				if(f.isDirectory()){
					log.info("Parsing query type {}",f.getName());
					File []  queryOps = f.listFiles();
					
					
					for(File queryOp: queryOps){
						HashMap<String, QueryResultSummary> versionMap = new HashMap<String, QueryResultSummary>();
						if(queryOp.isDirectory()){
							log.info(" Parsing query op {}",queryOp.getName());
							parse(queryOp,versionMap);
						}
						for(Entry<Integer,String> ent: QueryResultSummary.labels.entrySet()){
							File statsFile = new File(outDir,f.getName()+"-"+queryOp.getName()+"-"+ent.getValue()+".stats");
							FileOutputStream fos = new FileOutputStream(statsFile);
							StringBuilder sb = new StringBuilder();
							for(Entry<String, QueryResultSummary> ent1: versionMap.entrySet()){
								sb.append((ent1.getKey()+" "+ent1.getValue().toString(ent.getKey())+"\n"));
							}
							fos.write(sb.toString().getBytes());
							fos.close();
							statsFile = new File(outDir,f.getName()+"-all-"+ent.getValue()+".stats");
							if(statsFile.exists())fos = new FileOutputStream(statsFile,true);
							else fos = new FileOutputStream(statsFile);
							sb = new StringBuilder();
							int counter = 0;
							for(Entry<String, QueryResultSummary> ent1: versionMap.entrySet()){
								sb.append(counter++).append(" ").append(ent1.getKey()).append("-").append(queryOp.getName().trim()).append(" ").append(ent1.getValue().toString(ent.getKey())+"\n");
							}
							fos.write(sb.toString().getBytes());
							fos.close();
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void parse(File queryOp, HashMap<String, QueryResultSummary> versionMap) {
		
		File [] serQueries = queryOp.listFiles();
		for(File f: serQueries){
			if(f.isFile()){
				String prefix = f.getName().substring(0,f.getName().indexOf("."));
				if(f.getName().contains("no_opt_inv")) prefix+="no_opt_inv";
				if(f.getName().contains("no_opt_nested")) prefix+="no_opt_nested";
				QueryResultSummary qrs = versionMap.get(prefix);
				if(qrs == null) qrs = new QueryResultSummary();
				try{
				qrs.process(f);
				versionMap.put(prefix, qrs);
				}catch(Exception e){
					log.info("Exception {}:{} in {}", new Object[]{e.getClass().getSimpleName(), f,e.getMessage()});
				}
			}
		}
	}

	@Override
	public String getDescription() {
		// TODO Auto-generated method stub
		return "Get average benchmark results for all queries";
	}
}
