package qtree;
import ie.deri.urq.wods.hashing.us.PrefixTreeHashing;

import java.io.File;
import java.io.FileInputStream;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.semanticweb.wods.indexer.InsertCallback;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars2.rdfxml.RDFXMLParser;

import de.ilmenau.datasum.index.OnDiskOne4AllQTreeIndex;


public class QTreeOne4AllTEST extends TestCase {
	
	boolean storeDetailedCounts = false;

    public void testAddStatmentToIndex() {
	OnDiskOne4AllQTreeIndex index = new OnDiskOne4AllQTreeIndex(new PrefixTreeHashing(),storeDetailedCounts);
//	Node [] stmt = {new Resource("a"),new Resource("b"),new Resource("c"),new Resource("d")}; 
//	
//	Node [] stmt1 = {new Resource("http://data.semanticweb.org/person/andreas-harth"), 
//			 new Resource("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
//			 new Resource("http://xmlns.com/foaf/0.1/Person"),
//			 new Resource("http://data.semanticweb.org/person/andreas-harth/rdf")};
//	
//	  
//	index.addStatmentToIndex(stmt);
//	index.addStatmentToIndex(stmt1);   
//	
//	
//	Node [] stmt2 = {new Resource("http://dbpedia.org/resource/Ryan_Walter"),
//			 new Resource("http://dbpedia.org/ontology/birthplace"),
//			 new Resource("http://dbpedia.org/resource/British_Columbia"),
//			 new Resource("http://dbpedia.org/data/British_Columbia.xml")};
	
	Node [] stmt3 = {new Resource("http://axel.deri.ie/~axepol/foaf.rdf#me"),
		new Resource("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
		new Resource("http://xmlns.com/foaf/0.1/Person>"),
		new Resource("http://bblfish.net/people/henry/card")};
	
//	index.addStatmentToIndex(stmt2);
	index.addStatmentToIndex(stmt3); 
	assertEquals(4, index.getNmbOfSources());
    }
    
    public void testIndexStatementsFromFile() throws Exception {
	File in = new File("input/linked-data");
	
	List<File> testFiles = new ArrayList<File>();
	   {
	       testFiles.add(new File(in,"http%3A%2F%2Fdata.semanticweb.org%2Fperson%2Fandreas-harth%2Frdf"));
	       testFiles.add(new File(in,"http%3A%2F%2Fb4mad.net%2FFOAF%2Fgoern.rdf"));
	       testFiles.add(new File(in,"http%3A%2F%2Fbblfish.net%2Fpeople%2Fhenry%2Fcard"));
	      // testFiles.add(new File(in,"http%3A%2F%2Fdblp.l3s.de%2Fd2r%2Fdata%2Fauthors%2FAndrew_Philpot"));
	   }
	
	OnDiskOne4AllQTreeIndex index = new OnDiskOne4AllQTreeIndex(new PrefixTreeHashing(),storeDetailedCounts);
	 InsertCallback callback = new InsertCallback(index);
	
	 for(File file: testFiles){
	     RDFXMLParser parser = new RDFXMLParser(new FileInputStream(file),false,true,URLDecoder.decode(file.getName(),"UTF-8"),callback);
		Node[] quad;
		while(parser.hasNext()){
		    parser.next();
		//    index.addStatmentToIndex(parser.next());
		}     
	 }
	
	 System.out.println("QTree number of sources "+index._sourcesQTree.getRoot().getSourceIDs().size());
	
	assertEquals(testFiles.size(),index.getNmbOfSources());
	assertEquals(callback.insertedStatments(), (int) index._sourcesQTree.getRoot().getCount());
	
    }
    
    public void testIndexStatementsFromFilesInDir() throws Exception {
    	
		OnDiskOne4AllQTreeIndex index = new OnDiskOne4AllQTreeIndex(new PrefixTreeHashing(),storeDetailedCounts);
		
		InsertCallback callback = new InsertCallback(index);
		
		int insertedFiles=0;
		File in = new File("input/linked-data");
		for(File f: in.listFiles()){
			if(f.getName().startsWith("."))continue;	  
			System.err.println("Add "+f);
			insertedFiles++;
			RDFXMLParser parser = new RDFXMLParser(new FileInputStream(f),false,true,URLDecoder.decode(f.getName(),"UTF-8"),callback);
			
			Node[] quad;
			while(parser.hasNext()){
			    System.out.println(Nodes.toN3(parser.next()));
			//    index.addStatmentToIndex(parser.next());
			}
			
			
		    
		}
		assertEquals(insertedFiles,index.getNmbOfSources());
		assertEquals(callback.insertedStatments(), (int) index._sourcesQTree.getRoot().getCount());
	
    }

}
