
import ie.deri.urq.realidy.hashing.HashingFactory;
import ie.deri.urq.realidy.hashing.QTreeHashing;

import java.io.FileNotFoundException;
import java.util.Arrays;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;

public class Prefix {

    public static void main(String[] args) throws FileNotFoundException {
//	PrefixTree t = new PrefixTree();
//	t.init();
//	System.out.println(getIntValue(t.findKey("http://umbrich.net")));
////	t.printTree();
//	System.out.println(t.toString());
////	t.printTree();
//	
    	
    	QTreeHashing h = HashingFactory.createHasher("prefix1", new int[]{0,0,0}, new int[]{1000000,1000000,1000000});
    	System.out.println(
    			Arrays.toString(
    					
    					h.getHashCoordinates(new Node[]{new Resource("http://xmlns.com/foaf/spec"),new Resource("http://xmlns.com/foaf/spec"),new Resource("http://xmlns.com/foaf/spec")})
    					)
    				);
    	
    }
    
    protected static int getIntValue(String s) {
    	if (31 > s.length()) return Integer.parseInt(s, 2);
    	return Integer.parseInt(s.substring(0, 31), 2);
        }
}
