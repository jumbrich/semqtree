import ie.deri.urq.realidy.hashing.HashingFactory;
import ie.deri.urq.realidy.hashing.QTreeHashing;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;

import junit.framework.TestCase;

import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;

import sun.security.util.BigInt;


public class TestResource extends TestCase{
	public void testMain() throws Exception {
		int [] min = {0,0,0};
		int [] max = {100,100,100};
		
		
		assertEquals(0, scaleRange(0, 0, 1000, 0, 100));
		assertEquals(100, scaleRange(1000, 0, 1000, 0, 100));
//		assertEquals(10000000, scaleRange(100000000, 0, Integer.MAX_VALUE, 0, 10000000));
		assertEquals(0, scaleRange(0, 0, Integer.MAX_VALUE, 0, 100));
//		assertEquals(50, scaleRange(Integer.MAX_VALUE/2, 0, Integer.MAX_VALUE, 0, 100));
		assertEquals(50, scaleRange(-50, -1000, 1000, 0, 100));
//		Node [] n = new Node[]{new Resource("a"),new Resource("baasdfasdf"),new Resource("a")};
//		long start = System.currentTimeMillis();
//		InputStream is = new  FileInputStream(new File("/users/juum/Data/datasets/foaf_hop4.nq.gz"));
//		
//			is = new GZIPInputStream(is);
//		NxParser nxp = new NxParser(is);
//		while(nxp.hasNext()){Node [] stmt =nxp.next()œ; 
//		if(stmt[2] instanceof Literal){
//			System.out.println(Nodes.toN3(stmt));
//			System.out.println(Arrays.toString(h.getHashCoordinates(stmt)));œ
//		}
//		}is.close();
//		
//		long end = System.currentTimeMillis();
	}
	private static int rangeScale(int value, int min, int max){
		float oldMin = Integer.MIN_VALUE;
//		float oldMin = 0;
		float oldMax = Integer.MAX_VALUE;
		
		int res = (int) (value / (( oldMax-oldMin)/(max-min))+min);
		
		System.out.println("Scale "+value+" from ["+oldMin+","+oldMax+"] to "+res+" in ["+min+","+max+"]");
		return res;
	
	}
	
	static int scaleRange(int in, int oldMin, int oldMax, int newMin, int newMax)
	{
		in=in+oldMin;
		BigInteger i = BigInteger.valueOf(in);
//		System.out.println(BigInteger.valueOf(Integer.MAX_VALUE).subtract(BigInteger.valueOf(Integer.MIN_VALUE)));
//		System.out.println(Integer.MAX_VALUE-Integer.MIN_VALUE);
		float o =(oldMax -oldMin);
		float a =newMax -newMin;
		float oa = o/a;
		float ioa = in/(o/a);
//		float ioa2 = in*(a/0);
//		float io= in/o;
//		float ioa3= (in/o)*a;
//		
		System.out.println("in:"+in+"\no:"+o+"\nn:" +a+"\no/n:"+oa+"\ni/(o/n):"+ioa+"\nRES:"+(ioa+newMin));
		
		
		return (int) ioa;
	}
}
