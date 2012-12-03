package ie.deri.urq.realidy.cli;

import ie.deri.urq.realidy.bench.InsertBenchmark;
import ie.deri.urq.realidy.hashing.HashingFactory;
import ie.deri.urq.realidy.index.SemQTree;
import ie.deri.urq.realidy.insert.Indexer;
import ie.deri.urq.realidy.insert.InsertCallback;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.parser.ParseException;

public class Build extends CLIObject {

    private static long s_Serialise;
    private static long e_Serialise;
    private static long s_Insert;
    private static long e_Insert;
    private static int defaultThreads=2;
    private static int defaultHops=0;

    @Override
    public String getDescription() {
	// TODO Auto-generated method stub
	return "Build a new qtree";
    }

    public static void main(String[] args) {
    	Build b = new Build();
    	b.run(args);
	
    }

    private static void save(SemQTree idx, File indexFile) {
	s_Serialise = System.currentTimeMillis();
	idx.serialiseIndexToFile(indexFile);
	e_Serialise = System.currentTimeMillis();

    }

    private static void insert(SemQTree idx, CommandLine cmd) {
	s_Insert = System.currentTimeMillis();
	Callback cb = new InsertCallback(idx);
	String benchDir = cmd.getOptionValue(CLIObject.PARAM_BENCH_MARK_DIR);
	File benchFile = null;
	if(benchDir != null)
		benchFile = new File(benchDir);
		
//	if(benchFile !=null){
//		InsertBenchmark cb1;
//		try {
//			cb1 = new InsertBenchmark(cb,benchFile,30000L,false);
//			cb1.startBenchmark();
//			cb = cb1;
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}
//	
	
	if(idx != null && cmd.hasOption(CLIObject.PARAM_URI_INSERT)){
	    try {
	    	URI uri = new URI(cmd.getOptionValue(CLIObject.PARAM_URI_INSERT));
	    	
	    	int threads = (cmd.hasOption(CLIObject.PARAM_THREAD_NUMBER)) ? Integer.parseInt(cmd.getOptionValue(CLIObject.PARAM_THREAD_NUMBER)) : CLIObject.DEFAULT_THREAD_NUMBER;
			int hops = (cmd.hasOption(CLIObject.PARAM_MAX_CRAWLING_ROUNDS)) ?  Integer.parseInt(cmd.getOptionValue(CLIObject.PARAM_MAX_CRAWLING_ROUNDS)) :  CLIObject.DEFAULT_MAX_CRAWLING_ROUNDS;
	    	
	    	Indexer.insertFromWeb(uri, cb, threads, hops);
	    } catch (URISyntaxException e) {
	    	System.err.println("Input uri "+cmd.getOptionValue(CLIObject.PARAM_URI_INSERT)+" is not a valid uri: "+e.getMessage());
	    }
	}
	else if(idx != null && cmd.hasOption(CLIObject.PARAM_LOCAL_INSERT)){
		try {
			
			Indexer.insertFromNXZ(new File(cmd.getOptionValue(CLIObject.PARAM_LOCAL_INSERT)), cb);
		} catch (Exception e) {
			System.err.println("Input file "+cmd.getOptionValue(CLIObject.PARAM_LOCAL_INSERT)+" caused: "+e.getMessage());
		}
	}
    }
//	if(benchFile != null) ((InsertBenchmark)cb).stopBenchmark();
//		e_Insert = System.currentTimeMillis();
//    }

  
	@Override
	protected void addOptions(Options opts) {
		opts.addOption(CLIObject.OPTION_BUCKET_NUMBER);
		opts.addOption(CLIObject.OPTION_FANOUT_NUMBER);
		opts.addOption(CLIObject.OPTION_HASHER_NAME);
		opts.addOption(CLIObject.OPTION_LIST_HASHER);
		opts.addOption(CLIObject.OPTION_MAX_DIMENSION);
		opts.addOption(CLIObject.OPTION_STORE_DETAILED_COUNT);
		opts.addOptionGroup(CLIObject.OPTIONGROUP_INSERT_MODI);
		opts.addOption(CLIObject.OPTION_OUTPUT_DIR);
		opts.addOption(CLIObject.OPTION_BENCH_MARK_DIR);
		opts.addOption(CLIObject.OPTION_THREAD_NUMBER);
		opts.addOption(CLIObject.OPTION_MAX_CRAWLING_ROUNDS);
		
	}

	@Override
	protected void execute(CommandLine cmd) {
		if(cmd.hasOption(CLIObject.PARAM_LIST_HASHER)){
			System.err.println("Available hash functions:");
		    for(String s: HashingFactory.availableHashFunctions()){
		    	System.err.println("  "+s);
		    }
		    return;	
		}
		
		String hasher = (cmd.hasOption(CLIObject.PARAM_HASHER_NAME)) ? cmd.getOptionValue( CLIObject.PARAM_HASHER_NAME ) : CLIObject.DEFAULT_HASHER_NAME;
		
		int bucketNo = (cmd.hasOption(CLIObject.PARAM_BUCKET_NUMBER)) ? Integer.parseInt(cmd.getOptionValue(CLIObject.PARAM_BUCKET_NUMBER)) :  CLIObject.DEFAULT_BUCKET_NUMBER;
		
		int fanoutValue = (cmd.hasOption(CLIObject.PARAM_FANOUT_NUMBER)) ?  Integer.parseInt(cmd.getOptionValue(CLIObject.PARAM_FANOUT_NUMBER)) :  CLIObject.DEFAULT_FANOUT_NUMBER;
		
		int maxDimValue = (cmd.hasOption(CLIObject.PARAM_MAX_DIMENSION)) ? Integer.parseInt(cmd.getOptionValue(CLIObject.PARAM_MAX_DIMENSION)) :  CLIObject.DEFAULT_MAX_DIMENSION;
		
		boolean storeDetailedCounts = cmd.hasOption(CLIObject.PARAM_STORE_DETAILED_COUNT);

		File output = new File(cmd.getOptionValue(CLIObject.PARAM_OUTPUT_DIR));
		long start = System.currentTimeMillis(); 	
		SemQTree sqt = SemQTree.createSemQTreeSingleQTreeIndex(hasher, bucketNo, fanoutValue, 0, maxDimValue, storeDetailedCounts);
		
		insert(sqt,cmd);

		save(sqt,output);

		System.err.println("----------DONE------------");
		System.err.println("total: "+(System.currentTimeMillis()-start)+" ms");
		System.err.println(" insert: "+(e_Insert-s_Insert)+" ms");
		System.err.println(" save: "+(e_Serialise-s_Serialise)+" ms");

		
	}
}
