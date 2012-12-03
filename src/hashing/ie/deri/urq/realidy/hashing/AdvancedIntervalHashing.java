package ie.deri.urq.realidy.hashing;

import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;

public class AdvancedIntervalHashing extends QTreeHashing {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private QTreeHashing _stringHasher;
	private final double intervalRatio = 0.2D;
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
	public AdvancedIntervalHashing(String hasherName, int[] dimSpecMin,
			int[] dimSpecMax) {
		super(hasherName, dimSpecMin, dimSpecMax,false);
		_stringHasher = HashingFactory.createHasher(HashingFactory.HASHER_STRING_BPHASH,dimSpecMin,dimSpecMax);
		_prefixHasher = HashingFactory.createHasher(HashingFactory.HASHER_PREFIX,dimSpecMin,dimSpecMax);
	}

	public int objectHash(Node node) {
		if(node instanceof Literal) return scaleRange(_stringHasher.objectHash(node),0,Integer.MAX_VALUE,(int) (getDimSpecMax()[0]*intervalRatio), getDimSpecMax()[0]);
		else return scaleRange(_prefixHasher.objectHash(node), 0,Integer.MAX_VALUE,getDimSpecMin()[0], (int) ( getDimSpecMax()[0]*intervalRatio));
	}

	public int predicateHash(Node node) {
		return scaleRange(_stringHasher.predicateHash(node),0,Integer.MAX_VALUE, getDimSpecMin()[0], getDimSpecMax()[0]);
	}

	public int subjectHash(Node node) {
		return scaleRange(_prefixHasher.subjectHash(node),0,Integer.MAX_VALUE, getDimSpecMin()[0], getDimSpecMax()[0]);
	}
}
