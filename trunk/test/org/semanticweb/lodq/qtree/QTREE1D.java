/**
 *
 */
package org.semanticweb.lodq.qtree;


import java.io.File;
import java.io.FileInputStream;
import java.util.Vector;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.stats.Count;

import de.ilmenau.datasum.index.IntersectionInformation;
import de.ilmenau.datasum.index.QuerySpace;
import de.ilmenau.datasum.index.histogram.MultiDimHistogram;
import de.ilmenau.datasum.index.qtree.QTree;

/**
 * @author Juergen Umbrich (firstname.lastname@deri.org)
 * @date Apr 18, 2011
 */
public class QTREE1D extends TestCase{

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	
	public void test1D() throws Exception {
		int [] min = {0};
		int [] max = {Integer.MAX_VALUE};
		QTree qtree = new QTree(1, 10, 100, min, max, "0", "1", false);
		Vector<String> attrNames = new Vector<String>();
		attrNames.add("s");
		
//		int size = 50000000;
		long s = System.currentTimeMillis();
//		for(int i =0; i< size; i++){
//			int [] d = {(int) (Math.random()*100000)};
//			qtree.insertDataItem(d, "");
//		}	
		
		
		Count<Integer> c1 = new Count<Integer>();
		NxParser nxp = new NxParser(new FileInputStream(new File("/Users/juum/Data/datasets/4.nq")));
		int size =0;
		int t=0;
		while(nxp.hasNext()){
			t++;
			Node [] n = nxp.next();
			if(!(n[0] instanceof Literal)){
				int [] d = {(int) (Math.abs(n[0].toString().hashCode()))};
				c1.add(Math.abs(n[0].toString().hashCode()));
				qtree.insertDataItem(d, "");
				Vector<IntersectionInformation> v = qtree.getAllBucketsInQuerySpace(new QuerySpace(d, d, false));
				if(v.size() == 0) System.err.println("OHOHOHOHOHO");
				
				size++;
			}
			if(!(n[2] instanceof Literal)){
				int [] d = {(int) (Math.abs(n[2].toString().hashCode()))};
				c1.add(Math.abs(n[2].toString().hashCode()));
				qtree.insertDataItem(d, "");
				Vector<IntersectionInformation> v = qtree.getAllBucketsInQuerySpace(new QuerySpace(d, d, false));
				if(v.size() == 0) System.err.println("OHOHOHOHOHO");
				
				size++;
			}
			
			
		}
		System.out.println(((double)size/(double)(System.currentTimeMillis()-s)) +" inserts/ms ("+size+"/"+(System.currentTimeMillis()-s));
		qtree.printme();
		System.out.println(t+" total "+c1.keySet().size()+" distinct "+c1.getTotal());
		
		
		
	}
}
