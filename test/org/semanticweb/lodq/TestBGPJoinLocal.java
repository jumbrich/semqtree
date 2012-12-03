package org.semanticweb.lodq;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.HashSet;
import java.util.Set;

import junit.framework.TestCase;

import org.semanticweb.lods.query.joins.BGPMatcher;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.parser.ParseException;
import org.semanticweb.yars2.rdfxml.RDFXMLParser;

/**
 * test case evaluates bgp's and computes hash join for queries SampleQueries
 * on on-disk data
 * 
 * @author aharth
 *
 */
public class TestBGPJoinLocal extends TestCase {
	public static String DATA_DIR = "input/linked-data/";
	
	
	public void testBGP() throws Exception {
		long time = System.currentTimeMillis();
		
		File dir = new File(DATA_DIR);
		String[] sources = dir.list();
				
		for (int i = 0; i < SampleJoinQueries.QUERIES.length; i++) {			
			Node[][] q = SampleJoinQueries.QUERIES[i];
			int[][] join = SampleJoinQueries.QJ[i];
			
			Set<Node[]> current = evaluateBgp(sources, q[0]);
			
			for (int j = 1; j < q.length; j++) {
				Set<Node[]> gpresults = evaluateBgp(sources, q[j]);			
				current = computeJoin(current, join[j-1][0], gpresults, join[j-1][1]);
			}
			
//			for (Node[] nx : current) {
//				System.out.println(Nodes.toN3(nx));
//			}
			System.out.println("query yields " + current.size() + " results");
		}
		
		long time1 = System.currentTimeMillis();

		System.err.println("time elapsed: " + (time1-time) + " ms");
	}
	
	public Set<Node[]> computeJoin(Set<Node[]> l, int lpos, Set<Node[]> r, int rpos) {
		Set<Node[]> result = new HashSet<Node[]>();
		
		for (Node[] lnx : l) {
			Node ljc = lnx[lpos];
			for (Node[] rnx : r) {
				Node rjc = rnx[rpos];
				
				if (ljc.equals(rjc)) {
					Node[] comb = new Node[lnx.length+rnx.length];
					System.arraycopy(lnx, 0, comb, 0, lnx.length);
					System.arraycopy(rnx, 0, comb, lnx.length, rnx.length);

					result.add(comb);
				}
			}
		}
		
		return result;
	}
	
	public Set<Node[]> evaluateBgp(String[] sources, Node[] bgp) throws FileNotFoundException, ParseException, IOException {
		BGPMatcher m = new BGPMatcher(bgp);

		System.out.println("(sub)query for " + Nodes.toN3(bgp));

		Set<Node[]> results = new HashSet<Node[]>();

		for (String s : sources) {
			String baseurl = URLDecoder.decode(s, "utf-8");

			File f = new File(DATA_DIR + s);
			if (f.isFile()) {
				RDFXMLParser r = new RDFXMLParser(new FileInputStream(DATA_DIR + s), baseurl);

				while (r.hasNext()) {
					Node[] nx = r.next();
					if (m.match(nx)) {
						results.add(nx);
						//System.out.println(Nodes.toN3(nx));
					}
				}
			}
		}
		
		return results;
	}
}
