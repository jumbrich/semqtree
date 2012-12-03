package edu.kit.aifb.semqtree.vldbpeople;

import ie.deri.urq.realidy.index.SemQTree;
import ie.deri.urq.realidy.query.arq.QueryExecutor;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

import junit.framework.TestCase;

import org.semanticweb.yars.nx.Resource;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

public class StreamJoinTest extends TestCase {
	public void testQuery() throws Exception {
		File qtree = new File("input/vldb-people/alist/mixed.qtree.ser");

		SemQTree sqt =	SemQTree.loadIndex(qtree);
		System.out.println(sqt.info());
		sqt.enableDebugMode(true);

		long start = System.currentTimeMillis();
		QueryExecutor executor = new QueryExecutor(sqt,true);
		String queryString = "prefix foaf: <http://xmlns.com/foaf/0.1/>" +
		"\nprefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
		"\nSELECT ?uri ?knows " +
		"\nWHERE {\n" +
		"\t?uri rdf:type foaf:Agent ." +
		"\t?uri foaf:knows ?knows ." +
		"}";
		
		ResultSet set = executor.executeQuery(queryString, 25, 20, null);
		//ResultSetFormatter.outputAsCSV(set);
		
		Set<Resource> s = new HashSet<Resource>();
		
		while (set.hasNext()) {
			QuerySolution sol = set.next();
			s.add(new Resource(sol.get("?uri").toString()));
			s.add(new Resource(sol.get("?knows").toString()));
		}
		
		System.out.println(s);

		System.err.println("time elapsed "+(System.currentTimeMillis()-start)+" ms");
		
		URL u = new URL("http://sw.deri.org/~aharth/cgi-bin/tweets");
		
		HttpURLConnection con = (HttpURLConnection)u.openConnection();
		con.connect();
		
		InputStream is = con.getInputStream();
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		
		String line = null;
		
		while ((line = br.readLine()) != null) {
			if (line.startsWith("@prefix")) {
				System.out.println(line);
			} else {
				for (Resource match : s) {
					if (line.contains(match.toN3())) {
						System.out.println(match + ": " + line);
					}
				}
			}
		}
	}
}
