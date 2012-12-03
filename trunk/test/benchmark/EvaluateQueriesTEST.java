import java.io.File;

import ie.deri.urq.realidy.bench.Benchmark;
import ie.deri.urq.realidy.bench.IndexQueryBenchmark;
import ie.deri.urq.realidy.bench.QueryJenaBenchmark;
import ie.deri.urq.realidy.bench.QueryNXBenchmark;
import ie.deri.urq.realidy.bench.utils.BenchmarkConfig;
import ie.deri.urq.realidy.cli.EvaluateQueries;
import ie.deri.urq.realidy.hashing.HashingFactory;
import ie.deri.urq.realidy.hashing.QTreeHashing;
import junit.framework.TestCase;


public class EvaluateQueriesTEST extends TestCase{

	public void testMain() throws Exception {
		File index = new File("evaluation/vldb-people/qtree.prefix.ser");
		File queryFile = new File("evaluation/vldb-people/qtree-prefix.ser");
		File inputFile = new File("evaluation/vldb-people/qtree-prefix.ser");
		int max = 1000000;
		QTreeHashing hashing = HashingFactory.createHasher(HashingFactory.HASHER_PREFIX, new int[]{0,0,0}, new int[]{max,max,max});
		
		BenchmarkConfig config = BenchmarkConfig.read("/Users/juum/Data/semqtree/vldb_path_100_2.config");
		
//		Benchmark bench = new IndexQueryBenchmark(config, "qtree", hashing);
//		bench.benchmarkDisk();
//		bench.benchmarkPlot();
//		
//		bench = new QueryJenaBenchmark(config, "qtree", hashing);
//		bench.benchmarkDisk();
//		bench.benchmarkPlot();
		
		
		Benchmark bench = new QueryNXBenchmark(config, "qtree", hashing);
		bench.benchmarkDisk();
		bench.benchmarkPlot();
		
	}
	
	
}
