import ie.deri.urq.realidy.bench.Benchmark;
import ie.deri.urq.realidy.bench.IndexQueryBenchmark;
import ie.deri.urq.realidy.bench.QueryNXBenchmark;
import ie.deri.urq.realidy.index.SemQTree;
import java.io.File;
import junit.framework.TestCase;

public class BenchmarkQueryTest extends TestCase{
	
	public void testMain() throws Exception {
		String prefix = "evaluation/vldb-people/";
		
		File qtree = new File("input/qtrees/qtree-prefix.ser");
		File query = new File("test.sparql");
		File inputData = new File("input/data-all-04-clean.0.1.nq.gz");
		File outRoot = new File("tmpBench");
		outRoot.mkdirs();
//		
		SemQTree q = SemQTree.loadIndex(qtree);
		Benchmark bench = new IndexQueryBenchmark(q,query,inputData,outRoot);
		bench.benchmarkDisk();
		bench.benchmarkPlot();
		System.out.println(bench.summary());
		bench = new QueryNXBenchmark(q,query,inputData,outRoot);
		bench.benchmark();
		bench.benchmarkPlot();
		System.out.println(bench.summary());
	}
}