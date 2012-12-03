package ie.deri.urq.realidy.hashing;

import org.semanticweb.yars.nx.Node;


public class MixedHashing extends QTreeHashing{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private QTreeHashing _stringHasher;
	private QTreeHashing _prefixHasher;


	public MixedHashing(String hasherPrefix, int[] dimSpecMin, int[] dimSpecMax) {
		super(hasherPrefix,dimSpecMin, dimSpecMax,false);
		_stringHasher = HashingFactory.createHasher(HashingFactory.HASHER_STRING_BPHASH,dimSpecMin,dimSpecMax);
		_prefixHasher = HashingFactory.createHasher(HashingFactory.HASHER_PREFIX,dimSpecMin,dimSpecMax);
	}
		
	public int objectHash(Node s) {
//		System.out.println("Object-hash ["+s+"]= "+_prefixHasher.objectHash(s));
		return _prefixHasher.objectHash(s); 
	}

	public int predicateHash(Node s) {
//		System.out.println(s);
//		System.out.println(_stringHasher.predicateHash(s));
		return scaleRange(_stringHasher.predicateHash(s), 0,Integer.MAX_VALUE, getDimSpecMin()[0], getDimSpecMax()[0]);
	}

	public int subjectHash(Node s) {
//		System.out.println("Subject-hash ["+s+"]= "+_prefixHasher.subjectHash(s));
		return _prefixHasher.subjectHash(s);
	}
}