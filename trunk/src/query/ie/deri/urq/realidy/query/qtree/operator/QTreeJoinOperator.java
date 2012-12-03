package ie.deri.urq.realidy.query.qtree.operator;

import java.util.ArrayList;

import org.semanticweb.yars.stats.Count;

import de.ilmenau.datasum.index.AbstractIndex;
import de.ilmenau.datasum.index.Bucket;
import de.ilmenau.datasum.index.JoinSpace;

abstract public class QTreeJoinOperator {

	abstract public  JoinSpace  execute(AbstractIndex idx, JoinSpace left, int lpos,ArrayList<Bucket> right, int rpos, int joinLevel, boolean storeDetailCount);
	
	
	//debugging 
	//can be removed when done
	protected  Count<Integer> _jbDist = new Count<Integer>();
	protected  boolean _jbdEnable = true;;

	public  void enableJoinBucketDist(boolean enable){
		_jbdEnable = enable;
	}
	
	public  void resetJoiNBucketDist(){
		_jbDist = new Count<Integer>();
	}
	
	public  Count<Integer> getJoinBucketDist(){
		return _jbDist;
	}
}
