package ie.deri.urq.realidy.index;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import junit.framework.TestCase;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;

import de.ilmenau.datasum.exception.QTreeException;

public class SchemaIndexTest extends TestCase {

	public void testSmall() throws ParseException, IOException, QTreeException {
		String fname = "input/4_src.nq.gz";
		
		IndexInterface index = new SchemaIndex();
		
		InputStream is = new FileInputStream(fname);
		if (fname.endsWith(".gz")) {
			is = new GZIPInputStream(is);
		}
		
		NxParser nxp = new NxParser(is);
		
		while (nxp.hasNext()) {
			Node[] nx = nxp.next();
			
			index.addStatment(nx);
		}
		
		System.out.println(index.getRelevantSourcesForQuery("SELECT * WHERE { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> . }"));
    }
	
	public void testLarge() throws ParseException, IOException, QTreeException {
		String fname = "input/data-all-04-clean.0.1.nq.gz";
		
		IndexInterface index = new SchemaIndex();
		
		InputStream is = new FileInputStream(fname);
		if (fname.endsWith(".gz")) {
			is = new GZIPInputStream(is);
		}
		
		NxParser nxp = new NxParser(is);
		
		while (nxp.hasNext()) {
			Node[] nx = nxp.next();
			
			index.addStatment(nx);
		}
		
		System.out.println(index.getRelevantSourcesForQuery("SELECT * WHERE { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> . }"));
    }

}
