package ie.deri.urq.realidy.hashing;

import java.io.Serializable;
import java.util.Arrays;


import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.Variable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.ilmenau.datasum.index.AbstractIndex;

public abstract class QTreeHashing implements Serializable{
	private final static Logger log = LoggerFactory.getLogger(QTreeHashing.class);
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int[] _dimSpecMin;
	private int[] _dimSpecMax;
	private String _name;
	private boolean _autoScale;

	public int [] getMaxDim(){return _dimSpecMax;}	
	public int [] getMinDim(){return _dimSpecMin;}
	
	public QTreeHashing(String hasherName, int[] dimSpecMin, int [] dimSpecMax, boolean autoScale) {
		_dimSpecMin = dimSpecMin;
		_dimSpecMax = dimSpecMax;
		_name = hasherName;
		_autoScale = autoScale;
		log.info("Init hasher {} {} - {} autoScale:{}",new Object[]{_name, Arrays.toString(_dimSpecMin), Arrays.toString(_dimSpecMax),_autoScale});
	}

	public String getHasherName(){
		return _name;
	}
	protected int[] getDimSpecMin(){
		return _dimSpecMin;
	}
	protected int[] getDimSpecMax(){
		return _dimSpecMax;
	}

	/**
	 * compute hash coordinates
	 * 
	 * @param item
	 * @return
	 */
	public int[] getHashCoordinates(Node[] item){
//		System.out.println("INPUT: "+Nodes.toN3(item));
		int [] coord = new int[3];
		
		if(item[0] instanceof Variable) coord[0] = AbstractIndex.VARIABLE;
		else if(_autoScale) coord[0] = scaleRange(subjectHash(item[0]),getDimSpecMin()[0],getDimSpecMax()[0]);
		else coord[0] = ensureRange(subjectHash(item[0]),getDimSpecMin()[0],getDimSpecMax()[0]);
		
		if(item[1] instanceof Variable) coord[1] = AbstractIndex.VARIABLE;
		else if(_autoScale) coord[1] = scaleRange(predicateHash(item[1]),getDimSpecMin()[1],getDimSpecMax()[1]);
		else coord[1] = ensureRange(predicateHash(item[1]),getDimSpecMin()[1],getDimSpecMax()[1]);
		
		if(item[2] instanceof Variable) coord[2] = AbstractIndex.VARIABLE;
		else if(_autoScale) coord[2] = scaleRange(objectHash(item[2]),getDimSpecMin()[2],getDimSpecMax()[2]);
		else coord[2] = ensureRange(objectHash(item[2]),getDimSpecMin()[2],getDimSpecMax()[2]);
		
		
//		System.out.println("OUTPUT: "+Arrays.toString(coord));
		return coord;
	}

	private int ensureRange(int subjectHash, int i, int j) {
		if((subjectHash < i) || (subjectHash>j)){
			System.out.println("ensureRange:"+subjectHash);
			return scaleRange(subjectHash, i, j);
		}
		return subjectHash;
	}

	public abstract int subjectHash(Node s);
	public abstract int predicateHash(Node s);
	public abstract int objectHash(Node s);
	
	protected int scaleRange(int in, int newMin, int newMax){
		
		return scaleRange(in, 0, Integer.MAX_VALUE, newMin, newMax);
		
	}
	protected int scaleRange(int in, int oldMin, int oldMax, int newMin, int newMax)
	{
//		System.out.println("in:"+in);
		if(in<0) in =in*-1;
//		log.info("in:{} oldDiff:{} newDiff:{}", new Object[]{in,oldMax-oldMin,newMax-newMin});
		
		int value = new Float(((in / ((oldMax - oldMin) / (newMax - newMin))) + newMin)).intValue();
//		log.info("Scale ["+oldMin+","+oldMax+"] "+in+" -> "+value+" ["+newMin+","+newMax+"]");
		if(value<0) value=value*-1;
		
		return value;
	}
	
	public static int scaleRange1(int in, int oldMin, float oldMax, float newMin, float newMax){
		int value = new Float(((in / ((oldMax - oldMin) / (newMax - newMin))) + newMin)).intValue();
		if(value<0) value=value*-1;
		
//		
		
		return value;
	}
	
	@Override
	public String toString() {
		return this.getClass().getSimpleName()+"["+_dimSpecMin[0]+":"+_dimSpecMax[0]+"]";
	}
}