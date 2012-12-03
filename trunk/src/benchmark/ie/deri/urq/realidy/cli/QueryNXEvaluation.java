package ie.deri.urq.realidy.cli;

import ie.deri.urq.realidy.bench.utils.NXBenchSummary;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryNXEvaluation extends CLIObject{
	private static final Logger log = LoggerFactory.getLogger(QueryNXEvaluation.class);

	@Override
	protected void addOptions(Options opts) {
		opts.addOption(CLIObject.OPTION_GENERIC_INPUT);
		opts.addOption(CLIObject.OPTION_GENERIC_OUTPUT);
	}

	public static void main(String[] args) {
		QueryNXEvaluation e = new QueryNXEvaluation();
		e.run(new String[]{"-i", "/Users/juum/Tmp/evalDumps/all/queries" ,"-o", "/Users/juum/Tmp/evalDumps/all/eval" });
	}

	@Override
	protected void execute(CommandLine cmd) {
		File outDir = new File(cmd.getOptionValue(CLIObject.PARAM_GENERIC_OUTPUT));
		if(!outDir.exists()) outDir.mkdirs();
		File inDir = new File(cmd.getOptionValue(CLIObject.PARAM_GENERIC_INPUT));

		File [] queryTypes = inDir.listFiles();
		for(File f: queryTypes){
			if(f.isDirectory()){
				log.info("Parsing query type {}",f.getName());
				File []  queryOps = f.listFiles();

				for(File queryOp: queryOps){
					HashMap<String, NXBenchSummary> versionMap = new HashMap<String, NXBenchSummary>();
					if(queryOp.isDirectory()){
						log.info(" Parsing query op {}",queryOp.getName());
						File nxEval = new File(queryOp,"nxeval"); 
						if(nxEval.exists()){
							log.info(" NxEval: {}",nxEval.getAbsolutePath());
							parse(nxEval,versionMap);	
						}
					}
					try {
						for(Entry<Integer,String> ent: NXBenchSummary.labels.entrySet()){
							File statsFile = new File(outDir,f.getName()+"-allNX-"+ent.getValue()+".stats");
							FileOutputStream fos =null;
							if(statsFile.exists())
								fos = new FileOutputStream(statsFile,true);
							else fos = new FileOutputStream(statsFile);
							StringBuilder sb = new StringBuilder();
							int count =0;
							for(Entry<String, NXBenchSummary> ent1: versionMap.entrySet()){
								sb.append(count++).append(" ").append(ent1.getKey()).append("-").append(queryOp.getName().trim()).append(" ").append(ent1.getValue().toString(ent.getKey())+"\n");
							}
							fos.write(sb.toString().getBytes());
							fos.close();
						}
					} catch (Exception e) {
						e.printStackTrace();
					}

				}
			}
		}
	}

	private void parse(File nxEval, HashMap<String, NXBenchSummary> versionMap) {
		File [] serQueries = nxEval.listFiles(new FilenameFilter() {
			public boolean accept(File dir, String name) {
				return name.endsWith("stats");
			}
		});
		for(File f: serQueries){
			if(f.isFile()){
				String prefix = f.getName().substring(0,f.getName().indexOf("."));
				System.out.println(prefix);
				NXBenchSummary qrs = new NXBenchSummary();
				if(qrs == null) qrs = new NXBenchSummary();
				qrs.process(f);
				versionMap.put(prefix, qrs);
			}
		}
	}

	@Override
	public String getDescription() {
		return "Summaries the results from a query folder";
	}
}