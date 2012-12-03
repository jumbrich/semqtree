package org.semanticweb.lodq;



import java.io.File;
import java.io.FileInputStream;
import java.net.URLDecoder;
import java.util.HashSet;
import java.util.Set;

import junit.framework.TestCase;

import org.semanticweb.wods.lodq.BGPMatcher;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars2.rdfxml.RDFXMLParser;

/**
 * test case evaluates simple bgp's
 * on on-disk data
 * 
 * @author aharth
 *
 */
public class TestBGPLocal extends TestCase {
	public static String DATA_DIR = "input/linked-data/";
		
	public void testBGP() throws Exception {
		long time = System.currentTimeMillis();
		
		File dir = new File(DATA_DIR);
		String[] sources = dir.list();
				
		for (Node[] q : SampleBasicQueries.QUERIES) {
			BGPMatcher m = new BGPMatcher(q);

			System.out.println("Query for " + Nodes.toN3(q));
			
			Set<String> results = new HashSet<String>();

			for (String s : sources) {
				String baseurl = URLDecoder.decode(s, "utf-8");

				File f = new File(DATA_DIR + s);
				if (f.isFile()) {
					RDFXMLParser r = new RDFXMLParser(new FileInputStream(DATA_DIR + s), baseurl);
					while (r.hasNext()) {
						Node[] nx = r.next();
						if (m.match(nx)) {
							results.add(baseurl);
							//System.out.println(Nodes.toN3(nx));
						}
					}
				}
			}
			
			System.out.println(results.size() + " matches, sources " + results);
		}
		long time1 = System.currentTimeMillis();

		System.err.println("time elapsed: " + (time1-time) + " ms");
	}
}
