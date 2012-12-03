package ie.deri.urq.realidy.hashing;

import org.openrdf.model.BNode;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;

public class AdvancedIntervalHashing2 extends QTreeHashing {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private QTreeHashing _stringHasher;
	private final double objIvalBNodeRatio = 0.333D;
	private final double objIvalLiteralRatio = 0.666D;
	private final double subjIvalBNodeRatio = 0.333D;
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
	public AdvancedIntervalHashing2(String hasherName, int[] dimSpecMin,
			int[] dimSpecMax) {
		super(hasherName, dimSpecMin, dimSpecMax,false);
		_stringHasher = HashingFactory.createHasher(HashingFactory.HASHER_STRING_BPHASH,dimSpecMin,dimSpecMax);
		_prefixHasher = HashingFactory.createHasher(HashingFactory.HASHER_PREFIX,dimSpecMin,dimSpecMax);
	}

		@Override
	public int objectHash(Node node) {
		if(node instanceof Literal) return scaleRange(_stringHasher.objectHash(node),0,Integer.MAX_VALUE,(int) (getDimSpecMax()[0]*objIvalLiteralRatio), getDimSpecMax()[0]);
		else if(node instanceof BNode) return scaleRange(_stringHasher.predicateHash(node),0,Integer.MAX_VALUE,(int) (getDimSpecMax()[0]*objIvalBNodeRatio), (int)(getDimSpecMax()[0]*objIvalLiteralRatio));
		else return scaleRange(_prefixHasher.objectHash(node), 0, Integer.MAX_VALUE, getDimSpecMin()[0], (int) ( getDimSpecMax()[0]*objIvalBNodeRatio));
	}

	@Override
	public int predicateHash(Node s) {
		return scaleRange(_stringHasher.predicateHash(s), 0,Integer.MAX_VALUE,getDimSpecMin()[0], getDimSpecMax()[0]);
	}

	@Override
	public int subjectHash(Node node) {
		 if(node instanceof BNode) return scaleRange(_stringHasher.subjectHash(node),0,Integer.MAX_VALUE,(int) (getDimSpecMax()[0]*subjIvalBNodeRatio), getDimSpecMax()[0]);
		 else return scaleRange(_prefixHasher.subjectHash(node), 0,Integer.MAX_VALUE,getDimSpecMin()[0], (int) ( getDimSpecMax()[0]*subjIvalBNodeRatio));
	}
}