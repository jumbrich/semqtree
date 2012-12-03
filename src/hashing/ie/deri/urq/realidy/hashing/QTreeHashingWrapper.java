package ie.deri.urq.realidy.hashing;

import java.util.Collection;
import java.util.Set;

import org.semanticweb.yars.nx.Node;

public class QTreeHashingWrapper extends QTreeHashing {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String _hf;

	public QTreeHashingWrapper(String hashfunction, int[] dimSpecMin, int[] dimSpecMax){
		super(hashfunction,dimSpecMin,dimSpecMax,true);
		_hf = hashfunction;
	}

	@Override
	public int objectHash(Node s) {
		return (int) hash(s.toString());
	}

	@Override
	public int predicateHash(Node s) {
		return (int) hash(s.toString());
	}
	
	@Override
	public int subjectHash(Node s) {
		return (int) hash(s.toString());
	}
	
	public long hash(String value) {
		if(_hf.equals("rshash"))
			return rshash(value);
		if(_hf.equals("jshash"))
			return jshash(value);
		if(_hf.equals("pjwhash"))
			return pjwhash(value);
		if(_hf.equals("elfhash"))
			return elfhash(value);
		if(_hf.equals("bkdrhash"))
			return bkdrhash(value);
		if(_hf.equals("sdbmhash"))
			return sdbmhash(value);
		if(_hf.equals("djbhash"))
			return djbhash(value);
		if(_hf.equals("dekhash"))
			return dekhash(value);
		if(_hf.equals("bphash"))
			return bphash(value);
		if(_hf.equals("fnvhash"))
			return fnvhash(value);
		if(_hf.equals("aphash"))
			return aphash(value);
		return 0;
	}


	/*
	 **************************************************************************
	 *                                                                        *
	 *          General Purpose Hash Function Algorithms Library              *
	 *                                                                        *
	 * Author: Arash Partow - 2002                                            *
	 * URL: http://www.partow.net                                             *
	 * URL: http://www.partow.net/programming/hashfunctions/index.html        *
	 *                                                                        *
	 * Copyright notice:                                                      *
	 * Free use of the General Purpose Hash Function Algorithms Library is    *
	 * permitted under the guidelines and in accordance with the most current *
	 * version of the Common Public License.                                  *
	 * http://www.opensource.org/licenses/cpl.php                             *
	 *                                                                        *
	 **************************************************************************
	 */
	
	int b_rshash     = 378551;
	int a_rshash     = 63689;
	public long rshash(String str)
	{
		
		long hash = 0;

		for(int i = 0; i < str.length(); i++)
		{
			hash = hash * a_rshash + str.charAt(i);
			a_rshash    = a_rshash * b_rshash;
		}

		return hash;
	}
	/* End Of RS Hash Function */


	long hash_jshash = 1315423911;
	public long jshash(String str)
	{
		long hash = hash_jshash;

		for(int i = 0; i < str.length(); i++)
		{
			hash ^= ((hash << 5) + str.charAt(i) + (hash >> 2));
		}

		return hash;
	}
	/* End Of JS Hash Function */


	long BitsInUnsignedInt_pjwhash = (long)(4 * 8);
	long ThreeQuarters_pjwhash     = (long)((BitsInUnsignedInt_pjwhash  * 3) / 4);
	long OneEighth_pjwhash         = (long)(BitsInUnsignedInt_pjwhash / 8);
	long HighBits_pjwhash          = (long)(0xFFFFFFFF) << (BitsInUnsignedInt_pjwhash - OneEighth_pjwhash);
	
	public long pjwhash(String str)
	{
		long hash              = 0;	
		long test              = 0;
		for(int i = 0; i < str.length(); i++)
		{
			hash = (hash << OneEighth_pjwhash) + str.charAt(i);

			if((test = hash & HighBits_pjwhash)  != 0)
			{
				hash = (( hash ^ (test >> ThreeQuarters_pjwhash)) & (~HighBits_pjwhash));
			}
		}

		return hash;
	}
	/* End Of  P. J. Weinberger Hash Function */


	public long elfhash(String str)
	{
		long hash = 0;
		long x    = 0;

		for(int i = 0; i < str.length(); i++)
		{
			hash = (hash << 4) + str.charAt(i);

			if((x = hash & 0xF0000000L) != 0)
			{
				hash ^= (x >> 24);
			}
			hash &= ~x;
		}

		return hash;
	}
	/* End Of ELF Hash Function */

	long seed_bkdrhash = 131;
	public long bkdrhash(String str)
	{
		 // 31 131 1313 13131 131313 etc..
		long hash = 0;

		for(int i = 0; i < str.length(); i++)
		{
			hash = (hash * seed_bkdrhash) + str.charAt(i);
		}

		return hash;
	}
	/* End Of BKDR Hash Function */


	public long sdbmhash(String str)
	{
		long hash = 0;

		for(int i = 0; i < str.length(); i++)
		{
			hash = str.charAt(i) + (hash << 6) + (hash << 16) - hash;
		}

		return hash;
	}
	/* End Of SDBM Hash Function */


	public long djbhash(String str)
	{
		long hash = 5381;

		for(int i = 0; i < str.length(); i++)
		{
			hash = ((hash << 5) + hash) + str.charAt(i);
		}

		return hash;
	}
	/* End Of DJB Hash Function */


	public long dekhash(String str)
	{
		long hash = str.length();

		for(int i = 0; i < str.length(); i++)
		{
			hash = ((hash << 5) ^ (hash >> 27)) ^ str.charAt(i);
		}

		return hash;
	}
	/* End Of DEK Hash Function */


	public long bphash(String str)
	{
		long hash = 0;

		for(int i = 0; i < str.length(); i++)
		{
			hash = hash << 7 ^ str.charAt(i);
		}

		return hash;
	}
	/* End Of BP Hash Function */

	long fnv_prime_fnvhash = 0x811C9DC5;
	public long fnvhash(String str)
	{
		long hash = 0;
		for(int i = 0; i < str.length(); i++)
		{
			hash *= fnv_prime_fnvhash;
			hash ^= str.charAt(i);
		}

		return hash;
	}
	/* End Of FNV Hash Function */


	public long aphash(String str)
	{
		long hash = 0xAAAAAAAA;

		for(int i = 0; i < str.length(); i++)
		{
			if ((i & 1) == 0)
			{
				hash ^= ((hash << 7) ^ str.charAt(i) * (hash >> 3));
			}
			else
			{
				hash ^= (~((hash << 11) + str.charAt(i) ^ (hash >> 5)));
			}
		}

		return hash;
	}
	/* End Of AP Hash Function */


	public static final Set<String> hashFunctions = new java.util.HashSet<String>();
	static{
		hashFunctions.add("rshash");
		hashFunctions.add("jshash");
		hashFunctions.add("pjwhash");
		hashFunctions.add("elfhash");
		hashFunctions.add("bkdrhash");
		hashFunctions.add("sdbmhash");
		hashFunctions.add("djbhash");
		hashFunctions.add("dekhash");
		hashFunctions.add("bphash");
		hashFunctions.add("fnvhash");
		hashFunctions.add("aphash");
	}
	public static Collection<? extends String> availableFunctions() {
		return hashFunctions; 
	}


	}
