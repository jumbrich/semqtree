import java.io.File;

import junit.framework.TestCase;

import ie.deri.urq.realidy.bench.utils.Printer;
import ie.deri.urq.realidy.index.SemQTree;


public class PrintBucketsTest extends TestCase{

	public void testMain() throws Exception {
		Printer b = new Printer();
		b.printBuckets(SemQTree.loadIndex(new File("fnvhash.qtree.ser")), new File("tmp.test.bench.pb"),new File("tmp.test.bench.pb.plots"));
	}
}
