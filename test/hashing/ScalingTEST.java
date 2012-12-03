import java.util.Arrays;

import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Resource;

import sun.tools.jstat.Literal;

import com.hp.hpl.jena.graph.Node;

import ie.deri.urq.realidy.hashing.HashingFactory;
import ie.deri.urq.realidy.hashing.QTreeHashing;


public class ScalingTEST {

	
	public static void main(String[] args) {
		int oldMin = 0;
		int oldMax = Integer.MAX_VALUE;
		
		int newMin = 10000000;
		int newMax = 100000000;
		
		System.out.println(QTreeHashing.scaleRange1(Integer.MAX_VALUE/2, oldMin, oldMax, newMin, newMax));
		System.out.println(QTreeHashing.scaleRange1(0, oldMin, oldMax, newMin, newMax));
		
		
		System.out.println(QTreeHashing.scaleRange1(0, 0, 100, 1000, 10000));
		System.out.println(QTreeHashing.scaleRange1(50, 0, 10, 1000, 10000));
		long v = 100L;
		System.out.println(new Long(v).intValue());
		System.out.println((int) v);
		
		
		v = Integer.MAX_VALUE+100000;
		System.out.println(new Long(v).intValue());
		System.out.println((int) v);
		
		
		int [] dimSpecMin = {0,0,0};
		int [] dimSpecMax = {newMax,newMax,newMax};
		QTreeHashing p = HashingFactory.createHasher("prefix", dimSpecMin, dimSpecMax);
		System.out.println("----");
		QTreeHashing m = HashingFactory.createHasher("adv_int2", dimSpecMin, dimSpecMax);
		System.out.println("----");
		QTreeHashing a = HashingFactory.createHasher("adv_int", dimSpecMin, dimSpecMax);
		
		org.semanticweb.yars.nx.Node [] n= {new BNode("http://umbrich.net/"),new Resource("b"),new org.semanticweb.yars.nx.Literal("http://deri/ie/adfasdfasdfasdfasdf")};
		System.out.println(Arrays.toString(p.getHashCoordinates(n)));
		System.out.println(Arrays.toString(m.getHashCoordinates(n)));
		System.out.println(Arrays.toString(a.getHashCoordinates(n)));
		
		System.out.println("-------------");
		n[0] =new Resource("http://umbrich.net/");
		n[1] = new Resource("b");
		n[2] = new Resource("http://deri/ie/adfasdfasdfasdfasdf");
		
		System.out.println(Arrays.toString(p.getHashCoordinates(n)));
		System.out.println(Arrays.toString(m.getHashCoordinates(n)));
		System.out.println(Arrays.toString(a.getHashCoordinates(n)));
	}
}
