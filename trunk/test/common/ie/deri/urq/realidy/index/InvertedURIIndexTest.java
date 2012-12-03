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

public class InvertedURIIndexTest extends TestCase {

	public void testSmall() throws ParseException, IOException, QTreeException {
		String fname = "input/4_src.nq.gz";
		
		InvertedURIIndex index = new InvertedURIIndex();
		
		InputStream is = new FileInputStream(fname);
		if (fname.endsWith(".gz")) {
			is = new GZIPInputStream(is);
		}
		
		NxParser nxp = new NxParser(is);
		
		while (nxp.hasNext()) {
			Node[] nx = nxp.next();
			
			index.addStatment(nx);
		}
		
		System.out.println(index.getRelevantSourcesForQuery("SELECT * WHERE { <http://www.umbrich.net/foaf.rdf#me> ?p ?o . }"));
    }
	
	public void testLarge() throws ParseException, IOException, QTreeException {
		String fname = "input/data-all-04-clean.0.1.nq.gz";
		
		InvertedURIIndex index = new InvertedURIIndex();
		
		InputStream is = new FileInputStream(fname);
		if (fname.endsWith(".gz")) {
			is = new GZIPInputStream(is);
		}
		
		NxParser nxp = new NxParser(is);
		
		while (nxp.hasNext()) {
			Node[] nx = nxp.next();
			
			index.addStatment(nx);
		}
		
		System.out.println(index.getRelevantSourcesForQuery("SELECT * WHERE { <http://www.umbrich.net/foaf.rdf#me> ?p ?o . }"));
    }

}
