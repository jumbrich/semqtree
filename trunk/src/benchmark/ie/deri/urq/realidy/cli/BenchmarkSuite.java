package ie.deri.urq.realidy.cli;

import ie.deri.urq.realidy.bench.Benchmark;
import ie.deri.urq.realidy.bench.HashingBenchmark;
import ie.deri.urq.realidy.bench.IndexQueryBenchmark;
import ie.deri.urq.realidy.bench.IndexQueryInvBucketBenchmark;
import ie.deri.urq.realidy.bench.IndexQueryNonOptimisedBenchmark;
import ie.deri.urq.realidy.bench.InsertBenchmark;
import ie.deri.urq.realidy.bench.QueryNXBenchmark;
import ie.deri.urq.realidy.bench.utils.BenchmarkConfig;
import ie.deri.urq.realidy.hashing.HashingFactory;
import ie.deri.urq.realidy.hashing.QTreeHashing;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BenchmarkSuite extends CLIObject{
	private final static Logger log = LoggerFactory.getLogger(BenchmarkSuite.class);


	public static void main(String[] args) throws FileNotFoundException, IOException {
		//		BenchmarkSuite b = new BenchmarkSuite(new File("/Users/juum/Data/semqtree/new.small.config"));
		//		b.run(args);
		BenchmarkSuite b = new BenchmarkSuite();
		b.run(new String[]{"-?"});
	}

	@Override
	protected void execute(CommandLine cmd) {
		StringBuilder sb = new StringBuilder("==== Benchmark Summary =====\n");	
		BenchmarkConfig config;
		try {
			long start;
			config = BenchmarkConfig.read(cmd.getOptionValue(PARAM_BENCH_CONFIG_FILE));
			int dimMax = config.maxDim();
			config.rootDir().mkdirs();
			FileWriter fw = new FileWriter(new File(config.rootDir(),new File(cmd.getOptionValue(PARAM_BENCH_CONFIG_FILE)).getName()+".log"));
			if(!cmd.hasOption(PARAM_SKIP_HASH)){
				for(String hasher: config.hasher()){
					if(hasher.trim().length()!=0){
						QTreeHashing hashing = HashingFactory.createHasher(hasher, new int[]{0,0,0}, new int[]{dimMax,dimMax,dimMax});
						log.info("Loading Hasher {}", hashing);
						start = System.currentTimeMillis();
						run(new HashingBenchmark(config, null,hashing),config.reRun());
						fw.write("HashingBenchmark "+hasher+" in "+(System.currentTimeMillis()-start)+" ms\n");
					}
				}
			}
			for(String version: config.indexVersion()){
				if(version.equals("qtree") || version.equals("histo")){
					for(String hasher: config.hasher()){
						QTreeHashing hashing = HashingFactory.createHasher(hasher, new int[]{0,0,0}, new int[]{dimMax,dimMax,dimMax});
						log.info("Running version: {}",version);
						if(!cmd.hasOption(PARAM_SKIP_INSERT)){
							start = System.currentTimeMillis();
							run( new InsertBenchmark(config, version, hashing),config.reRun());
							fw.write("InsertBenchmark "+version+"-"+hasher+" in "+(System.currentTimeMillis()-start)+" ms\n");
						}
						if(!cmd.hasOption(PARAM_SKIP_NONOPT)){
							start = System.currentTimeMillis();
							run( new IndexQueryNonOptimisedBenchmark(config, version, hashing),config.reRun());
							fw.write("IndexQueryNonOptimisedBenchmark "+version+"-"+hasher+" in "+(System.currentTimeMillis()-start)+" ms\n");
						}
						if(!cmd.hasOption(PARAM_SKIP_INVIDX)){
							start = System.currentTimeMillis();
							run( new IndexQueryInvBucketBenchmark(config, version, hashing),config.reRun());
							fw.write("IndexQueryInvBucketBenchmark "+version+"-"+hasher+" in "+(System.currentTimeMillis()-start)+" ms\n");
						}
						if(!cmd.hasOption(PARAM_SKIP_NESTED)){
							start = System.currentTimeMillis();
							run( new IndexQueryBenchmark(config, version, hashing),config.reRun());
							fw.write("IndexQueryBenchmark "+version+"-"+hasher+" in "+(System.currentTimeMillis()-start)+" ms\n");
						}
						if(!cmd.hasOption(PARAM_SKIP_NX)){
							start = System.currentTimeMillis();
							run( new QueryNXBenchmark(config, version, hashing),config.reRun());
							fw.write("QueryNXBenchmark "+version+"-"+hasher+" in "+(System.currentTimeMillis()-start)+" ms\n");
						}
					}
				}else{
					start = System.currentTimeMillis();
					run( new InsertBenchmark(config, version, null),config.reRun());
					fw.write("InsertBenchmark "+version+"-no-hasher in "+(System.currentTimeMillis()-start)+" ms\n");

					start = System.currentTimeMillis();
					run( new IndexQueryBenchmark(config, version, null),config.reRun());
					fw.write("IndexQueryBenchmark "+version+"-no-hasher in "+(System.currentTimeMillis()-start)+" ms\n");

					start = System.currentTimeMillis();
					run( new QueryNXBenchmark(config, version, null),config.reRun());
					fw.write("QueryNXBenchmark "+version+"-no-hasher in "+(System.currentTimeMillis()-start)+" ms\n");

				}
			}
			fw.flush(); fw.close();
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	public void run(Benchmark bench, boolean rerun){
		log.info("{} run:{}",new Object[]{bench.getClass().getSimpleName(), rerun || !bench.exists()});
		long start = System.currentTimeMillis();
		if(bench.valid()){
			bench.benchmarkDisk();
			bench.benchmarkPlot();
		}
		log.info(">[{}]\n  [SUMMARY]\n{}\n<[{}] {} (ms)",new Object[]{bench.getClass().getSimpleName(),bench.summary(),bench.getClass().getSimpleName(), System.currentTimeMillis()-start});
		System.gc();
	}

	protected void addOptions(Options opts) {
		opts.addOption(OPTION_BENCH_CONFIG_FILE);
		opts.addOption(OPTION_SKIP_HASH);
		opts.addOption(OPTION_SKIP_INSERT);
		opts.addOption(OPTION_SKIP_NONOPT);
		opts.addOption(OPTION_SKIP_INVIDX);
		opts.addOption(OPTION_SKIP_NESTED);
		opts.addOption(OPTION_SKIP_NX);
	}


	public String getDescription() {
		return null;
	}

}
