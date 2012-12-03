package ie.deri.urq.realidy.cli;

import ie.deri.urq.realidy.bench.Measure;
import ie.deri.urq.realidy.hashing.HashingFactory;
import ie.deri.urq.realidy.hashing.QTreeHashing;
import ie.deri.urq.realidy.index.IndexFactory;
import ie.deri.urq.realidy.index.IndexInterface;
import ie.deri.urq.realidy.insert.InsertCallback;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InsertBenchmark extends CLIObject{
	private final static Logger log = LoggerFactory.getLogger(InsertBenchmark.class);

	

	private final Long ticTime = 10000L;
	private final Long stmtTic = 10000L;

	    	
    	
	
	
	@Override
	protected void addOptions(Options opts) {
		//each benchmark needs an output file
		opts.addOption(CLIObject.OPTION_GENERIC_OUTPUT);
		opts.addOption(CLIObject.OPTION_HASHER_NAME);
		opts.addOption(CLIObject.OPTION_MAX_DIMENSION);
		opts.addOption(CLIObject.OPTION_MAX_BUCKEST);
		opts.addOption(CLIObject.OPTION_MAX_FANOUT);
		opts.addOption(CLIObject.OPTION_STORE_DETAILED_COUNT);
		opts.addOption(CLIObject.OPTION_GENERIC_INPUT);
		opts.addOption(CLIObject.OPTION_INDEX_VERSION);
		opts.addOption(CLIObject.OPTION_REQUIRED_LOAD_INDEX);
	}
	@Override
	protected void execute(CommandLine cmd) {
		File outDir = new File(cmd.getOptionValue(CLIObject.PARAM_GENERIC_OUTPUT));
		if(!outDir.exists()) outDir.mkdirs();
		String hasher = cmd.getOptionValue(CLIObject.PARAM_HASHER_NAME);
		Integer maxDim= Integer.valueOf(cmd.getOptionValue(CLIObject.PARAM_MAX_DIMENSION,"0"));
		Integer maxBuckets= Integer.valueOf(cmd.getOptionValue(CLIObject.PARAM_MAX_BUCKETS,"0"));
		Integer maxFanout= Integer.valueOf(cmd.getOptionValue(CLIObject.PARAM_MAX_FANOUT,"0"));
		boolean storeDetailedCount= Boolean.parseBoolean(cmd.getOptionValue(CLIObject.PARAM_STORE_DETAILED_COUNT,"false"));
		File inputFile = new File(cmd.getOptionValue(CLIObject.PARAM_GENERIC_INPUT));
		String version = cmd.getOptionValue(CLIObject.PARAM_INDEX_VERSION);
		File indexOut = new File(cmd.getOptionValue(CLIObject.PARAM_LOAD_INDEX));
		File outTime = new File(outDir, version +"-"+ hasher +"_insert."+ ticTime +"ms.dat");
		File outStmt = new File(outDir, version +"-"+ hasher +"_insert."+ stmtTic +"stmts.dat");
		
		QTreeHashing hashing = HashingFactory.createHasher(hasher, new int[]{0,0,0}, new int[]{maxDim,maxDim,maxDim});
		IndexInterface idx = IndexFactory.createIndex(version, hashing, maxBuckets, maxFanout, maxDim, storeDetailedCount);
		
		InsertCallback cb = new InsertCallback(idx);
   	 	Measure bench=null;
		Measure benchStmt=null;
		try {
			long start= System.currentTimeMillis(); 
			benchStmt = new Measure(cb,outStmt,stmtTic,true);
			bench = new Measure(benchStmt,outTime,ticTime,false);
			bench.startDocument();
			bench.indexLocal(inputFile);
			bench.endDocument();
			start= System.currentTimeMillis(); 
			idx.serialiseIndexToFile(indexOut);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
		
	@Override
	public String getDescription() {
		return "benchmark hashing function";
	}
}
