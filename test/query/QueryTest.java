import ie.deri.urq.realidy.index.SemQTree;
import ie.deri.urq.realidy.query.arq.QueryExecutor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.sparql.resultset.ResultsFormat;
import com.hp.hpl.jena.sparql.vocabulary.FOAF;
import com.hp.hpl.jena.vocabulary.RDF;

import junit.framework.TestCase;
import de.ilmenau.datasum.index.QueryResultEstimation;


public class QueryTest extends TestCase{

	public void testQuery() throws Exception {
		File qtree = new File("qtree1.ser");
		File out = new File("tmp."+QueryTest.class.getSimpleName()+".testQuery");
		out.mkdirs();
		
		SemQTree sqt =	SemQTree.loadIndex(qtree);
		System.out.println(sqt.info());
//		sqt.enableDebugMode(true);
		
		
		long start = System.currentTimeMillis();
		QueryExecutor executor = new QueryExecutor(sqt,true);
		String queryString = "prefix foaf: <http://xmlns.com/foaf/0.1/>" +
				"\nprefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
				"\nSELECT ?uri " +
				"\nWHERE { " +
				"\n?uri rdf:type foaf:Person ." +
				"\n?uri ?p ?acc ." +
				"\n?uri foaf:knows ?acc2 ." +
				"\n?uri ?p ?acc3 ." +
//				"\n?acc foaf:accountServiceHomepage <http://twitter.com/> ." +
//				"\n?acc foaf:accountName \"juum\" .\n" +
						"}";
		queryString = readQueryFromFile(new File("/Users/juum/Tmp/path.7.sparql"));
//		queryString = "SELECT ?join1\n" +
//				"WHERE{\n?join1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .\n}";
//				
		sqt.enableDebugMode(true);
		QueryResultEstimation est = sqt.evaluateQuery(queryString);
		System.out.println(est.getRelevantSourcesRanked());

		//		FileWriter fw = new FileWriter(new File(out,"query1.bench"));
//		fw.write(est.toString());
//		fw.close();
//		System.out.println(est);
//		fw = new FileWriter(new File(out,"query1.rankedSrc"));
//		for(String s :est.getRelevantSourcesRanked()){
//			fw.write(s+"\n");
//			fw.flush();
//		}
//		fw.close();
//		ResultSet set = executor.executeQuery(queryString);
//		
//		ResultSetFormatter.outputAsJSON(System.out,  set);
		System.err.println("Time elapsed "+(System.currentTimeMillis()-start)+" ms");
	}

	private String readQueryFromFile(File file) throws IOException {
		StringBuilder sb = new StringBuilder();
		BufferedReader br = new BufferedReader(new FileReader(file));
		String line = null;
		while((line=br.readLine())!=null){
			sb.append(line).append("\n");
		}
		br.close();
		return sb.toString();
	}
}