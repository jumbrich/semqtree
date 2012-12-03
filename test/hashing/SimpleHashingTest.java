import java.util.Arrays;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;

import ie.deri.urq.realidy.hashing.HashingFactory;
import ie.deri.urq.realidy.hashing.QTreeHashing;
import junit.framework.TestCase;


public class SimpleHashingTest extends TestCase {

	
	public void testSimple() throws Exception {
		Resource s = new Resource("http://harth.org/andreas/foaf#ah");
		Resource p = new Resource("http://xmlns.com/foaf/0.1/knows");
		Resource o = new Resource("http://umbrich.net/foaf.rdf#me");
		int m = 10000000;
		QTreeHashing h = HashingFactory.createHasher("simple", new int[]{0,0,0}, new int[]{m,m,m});
		System.out.println(Arrays.toString(h.getHashCoordinates(new Node[]{s,p,o})));
	}
}
