import junit.framework.TestCase;
import ie.deri.urq.realidy.cli.BenchmarkQueries;


public class BenchmarkQueriesTest extends TestCase {
	
	public void testMain() throws Exception {
		BenchmarkQueries b = new BenchmarkQueries();
		b.run(new String[]{"-q","queries.2.txt","-idx","mixed.qtree.ser","-od","tmp.test.bench"});
	}
}
