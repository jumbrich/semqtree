package ie.deri.urq.realidy.cli;

import ie.deri.urq.realidy.bench.HashingBenchmark;
import ie.deri.urq.realidy.bench.utils.BenchmarkConfig;
import ie.deri.urq.realidy.hashing.HashingFactory;
import ie.deri.urq.realidy.hashing.QTreeHashing;

import java.io.File;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.semanticweb.yars.nx.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetHashDist extends CLIObject{
	private final static Logger log = LoggerFactory.getLogger(GetHashDist.class);

	@Override
	public String getDescription() {
		return "Get an overiew about how the distribution of the data for different hash functions";
	}

	@Override
	protected void addOptions(Options opts) {
		opts.addOption(OPTION_LOCAL_INSERT);
		opts.addOption(OPTION_LIST_HASHER);
		opts.addOption(OPTION_HASHER_NAME);
		opts.addOption(OPTION_MAX_DIMENSION);

		opts.addOption(OPTION_DEBUG);
		opts.addOption(OPTION_REQUIRED_OUTPUT_DIR);
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

		File localFile = new File(cmd.getOptionValue(PARAM_LOCAL_INSERT));
		File outDir = new File(cmd.getOptionValue(PARAM_OUTPUT_DIR));
		int maxDim = cmd.hasOption(PARAM_MAX_DIMENSION) ? Integer.valueOf(cmd.getOptionValue(PARAM_MAX_DIMENSION)) : 1000000 ;
		int [] minDim = {0,0,0};
		int [] maxDim1 = {maxDim,maxDim,maxDim};
		if(cmd.hasOption(PARAM_HASHER_NAME)){
			try {
				new File(outDir,"plots").mkdirs();
				runWithHasher(localFile, outDir,new File(outDir,"plots"),HashingFactory.createHasher(cmd.getOptionValue(PARAM_HASHER_NAME),minDim,maxDim1),cmd.hasOption(PARAM_DEBUG));
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		else{
			for(String hash: HashingFactory.availableHashFunctions()){	
				try {
					runWithHasher(localFile, outDir,new File(outDir,"plots"),HashingFactory.createHasher(hash, minDim, maxDim1), cmd.hasOption(PARAM_DEBUG));
				} catch (IOException e) {
					e.printStackTrace();
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public void runWithHasher(File localFile, File outDir, File plotRoot, QTreeHashing hashing, boolean debug) throws IOException, ParseException {
		log.info("<{}-Hashing> <- hashing function",hashing.getHasherName());
		long start = System.currentTimeMillis();
		BenchmarkConfig config = new BenchmarkConfig();
		config.setMaxDim(hashing.getMaxDim()[0]);
		config.setHashingRoot(outDir);
		config.setPlotRoot(plotRoot);
		config.setInputData(localFile);
		HashingBenchmark b = new HashingBenchmark(config, null, hashing);
		b.benchmarkDisk();
		b.benchmarkPlot();
		log.info("<{}-Hashing> Time elapsed: {} ms ",new Object[]{hashing.getHasherName(),(System.currentTimeMillis()-start)});	
	}
}
