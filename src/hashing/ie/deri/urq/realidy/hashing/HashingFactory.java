package ie.deri.urq.realidy.hashing;

import ie.deri.urq.realidy.hashing.us.MarioHashing;
import ie.deri.urq.realidy.hashing.us.PrefixTreeHashing;
import ie.deri.urq.realidy.hashing.us.PrefixTreeHashing1;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;


public class HashingFactory {

	/**
	 * 
	 */
	public static final String HASHER_PREFIX = "prefix";
	public static final String HASHER_PREFIX1 = "prefix1";
	/**
	 * 
	 */
	public static final String HASHER_STRING_MARIO = "mario";
	/**
	 * 
	 */
	public static final String HASHER_STRING_SIMPLE ="simple";

	public static final String HASHER_ADVANCED ="adv";
	public static final String HASHER_ADVANCEDNAMESPACE = "adv_ns";
	public static final String HASHER_ADVANCEDINTERVAL = "adv_int";
	public static final String HASHER_ADVANCEDINTERVAL2 = "adv_int2";
	/**
	 * 
	 */
	public static final String HASHER_MIXED = "mixed";
	/**
	 * http://www.partow.net/programming/hashfunctions/index.html   
	 */
	public static final String HASHER_STRING_RSHASH = "rshash";
	/**
	 * http://www.partow.net/programming/hashfunctions/index.html   
	 */
	public static final String HASHER_STRING_JSHASH = "jshash";
	/**
	 * http://www.partow.net/programming/hashfunctions/index.html   
	 */
	public static final String HASHER_STRING_PJWHASH = "pjwhash";
	/**
	 * http://www.partow.net/programming/hashfunctions/index.html   
	 */
	public static final String HASHER_STRING_ELFHASH = "elfhash";
	/**
	 * http://www.partow.net/programming/hashfunctions/index.html   
	 */
	public static final String HASHER_STRING_BKDRHASH ="bkdrhash";
	/**
	 * http://www.partow.net/programming/hashfunctions/index.html   
	 */
	public static final String HASHER_STRING_SDBMHASH ="sdbmhash";
	/**
	 * http://www.partow.net/programming/hashfunctions/index.html   
	 */
	public static final String HASHER_STRING_DJBHASH="djbhash";
	/**
	 * http://www.partow.net/programming/hashfunctions/index.html   
	 */
	public static final String HASHER_STRING_DEKHASH="dekhash";
	/**
	 * http://www.partow.net/programming/hashfunctions/index.html   
	 */
	public static final String HASHER_STRING_BPHASH="bphash";
	/**
	 * http://www.partow.net/programming/hashfunctions/index.html   
	 */
	public static final String HASHER_STRING_FNVHASH="fnvhash";
	/**
	 * http://www.partow.net/programming/hashfunctions/index.html   
	 */
	public static final String HASHER_STRING_APHASH ="aphash";

	public static final Set<String> hashFunctions = new HashSet<String>();
	static{
		hashFunctions.add(HASHER_PREFIX);
		//	hashFunctions.add(HASHER_STRING_MARIO);
		hashFunctions.add(HASHER_STRING_SIMPLE);
		hashFunctions.add(HASHER_MIXED);
		hashFunctions.add(HASHER_STRING_RSHASH);
		hashFunctions.add(HASHER_STRING_JSHASH);
		hashFunctions.add(HASHER_STRING_PJWHASH);
		hashFunctions.add(HASHER_STRING_ELFHASH);
		hashFunctions.add(HASHER_STRING_BKDRHASH);
		hashFunctions.add(HASHER_STRING_SDBMHASH);
		hashFunctions.add(HASHER_STRING_DJBHASH);
		hashFunctions.add(HASHER_STRING_DEKHASH);
		hashFunctions.add(HASHER_STRING_BPHASH);
		hashFunctions.add(HASHER_STRING_FNVHASH);
		hashFunctions.add(HASHER_STRING_APHASH);
		hashFunctions.add(HASHER_ADVANCEDNAMESPACE);
		hashFunctions.add(HASHER_ADVANCEDINTERVAL);
		hashFunctions.add(HASHER_ADVANCEDINTERVAL2);
		hashFunctions.add(HASHER_ADVANCED);
		hashFunctions.add(HASHER_PREFIX1);
	};

	public static Collection<String> availableHashFunctions(){
		return hashFunctions;
	}

	public static QTreeHashing createHasher(String hasher, int[] dimSpecMin,
			int[] dimSpecMax) {
		if(hasher.equals(HASHER_PREFIX))
			return new PrefixTreeHashing(HASHER_PREFIX,dimSpecMin,dimSpecMax);
		else if(hasher.equals(HASHER_PREFIX1))
			return new PrefixTreeHashing1(HASHER_PREFIX1,dimSpecMin,dimSpecMax);
		else if(hasher.equals(HASHER_STRING_MARIO))
			return new MarioHashing(HASHER_STRING_MARIO,dimSpecMin,dimSpecMax);
		else if(hasher.equals(HASHER_STRING_SIMPLE))
			return new SimpleHashing(HASHER_STRING_SIMPLE, dimSpecMin,dimSpecMax);
		else if(hasher.equals(HASHER_MIXED))
			return new MixedHashing(HASHER_MIXED,dimSpecMin,dimSpecMax);
		else if(hasher.equals(HASHER_ADVANCED))
			return new AdvancedHashing(HASHER_ADVANCED,dimSpecMin,dimSpecMax);
		else if(hasher.equals(HASHER_ADVANCEDNAMESPACE)){
			return new NamespaceOrStringHashing(HASHER_ADVANCEDNAMESPACE,dimSpecMin,dimSpecMax);
		}
		else if(hasher.equals(HASHER_ADVANCEDINTERVAL)){
			return new AdvancedIntervalHashing(HASHER_ADVANCEDINTERVAL,dimSpecMin,dimSpecMax);
		}
		else if(hasher.equals(HASHER_ADVANCEDINTERVAL2)){
			return new AdvancedIntervalHashing2(HASHER_ADVANCEDINTERVAL2,dimSpecMin,dimSpecMax);
		}
		else
			return new QTreeHashingWrapper(hasher,dimSpecMin,dimSpecMax);
	}
}
