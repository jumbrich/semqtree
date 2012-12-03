package ie.deri.urq.realidy.hashing;

import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.Variable;

import de.ilmenau.datasum.exception.NotYetImplementedException;
import de.ilmenau.datasum.index.AbstractIndex;
import ie.deri.urq.realidy.hashing.us.PrefixTreeHashing;

public class NamespaceOrStringHashing extends QTreeHashing {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private QTreeHashing _stringHasher;
	
	/**
	 * At the moment we hash 
	 *   subjects with the prefix hashing
	 *   predicates with string hashing
	 *   object literals with string hashing
	 *   object entities with string hashing
	 *  
	 * @param hasherName
	 * @param dimSpecMin
	 * @param dimSpecMax
	 */
	public NamespaceOrStringHashing(String hasherName, int[] dimSpecMin,
			int[] dimSpecMax) {
		super(hasherName, dimSpecMin, dimSpecMax,true);
		_stringHasher = HashingFactory.createHasher(HashingFactory.HASHER_STRING_BPHASH,dimSpecMin,dimSpecMax);
	}

	private static Node getNamespace(Node r){
		String url = r.toString();
		int hash, slash, end;
		hash = url.lastIndexOf("#");
		slash = url.lastIndexOf("/");
		end = Math.max(hash, slash);
		if(end <= 0)
			return r;
		else if(end == hash)
			return new Resource(url.substring(0,end));
		else
			return new Resource(url.substring(0,end+1));
	}


	public int objectHash(Node s) {
		return _stringHasher.objectHash(getNamespace(s));
	}

	public int predicateHash(Node s) {
		return _stringHasher.predicateHash(getNamespace(s));
	}

	public int subjectHash(Node s) {
		return _stringHasher.subjectHash(getNamespace(s));
	}
}