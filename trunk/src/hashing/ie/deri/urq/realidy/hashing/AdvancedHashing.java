package ie.deri.urq.realidy.hashing;

import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Variable;

import de.ilmenau.datasum.exception.NotYetImplementedException;
import de.ilmenau.datasum.index.AbstractIndex;
import ie.deri.urq.realidy.hashing.us.PrefixTreeHashing;

public class AdvancedHashing extends QTreeHashing {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private QTreeHashing _stringHasher;
	private QTreeHashing _prefixHasher;

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
	public AdvancedHashing(String hasherName, int[] dimSpecMin,
			int[] dimSpecMax) {
		super(hasherName, dimSpecMin, dimSpecMax,false);
		
		_stringHasher = HashingFactory.createHasher(HashingFactory.HASHER_STRING_BPHASH,dimSpecMin,dimSpecMax);
		_prefixHasher = HashingFactory.createHasher(HashingFactory.HASHER_PREFIX,dimSpecMin,dimSpecMax);
	}

	public int objectHash(Node node) {
		if(node instanceof Literal) return scaleRange(_stringHasher.objectHash(node), getDimSpecMin()[0], getDimSpecMax()[0]);
		else return scaleRange(_prefixHasher.objectHash(node), getDimSpecMin()[0], getDimSpecMax()[0]);
	}

	public int predicateHash(Node node) {
		return scaleRange(_stringHasher.predicateHash(node), getDimSpecMin()[0], getDimSpecMax()[0]);
	}

	
	public int subjectHash(Node node) {
		return scaleRange(_prefixHasher.subjectHash(node), getDimSpecMin()[0], getDimSpecMax()[0]);
	}

}
