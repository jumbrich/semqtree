package ie.deri.urq.realidy.query.qtree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.ilmenau.datasum.index.Bucket;

public class InvertedBucketIndex {
	private static final Logger log = LoggerFactory.getLogger(InvertedBucketIndex.class);
	Map<Integer,List<Bucket>> idx = null;
	
	public InvertedBucketIndex(ArrayList<Bucket> right, int index, int maxDim) {
		idx = new HashMap<Integer, List<Bucket>>(maxDim);
		for(Bucket b: right){
//			log.info("Add bucket {} to {} - {}",new Object[]{b,b.getLowerBoundaries()[index],b.getUpperBoundaries()[index]});
			for(int i = b.getLowerBoundaries()[index]; i <=b.getUpperBoundaries()[index];i++){
				add(b,i);
			}
		}
	}

	private void add(Bucket b, int i) {
		List<Bucket> bs= idx.get(i);
		if(bs == null) bs = new ArrayList<Bucket>();
		bs.add(b);
		idx.put(i,bs);
	}

	public Set<Bucket> getOverlap(int i, int j) {
		
		Set<Bucket> s = new HashSet<Bucket>();
		for(int a =i; a<=j;a++){
			if(idx.containsKey(a))
				s.addAll(idx.get(a));
		}
//		log.info("Determine {} buckets that overlap with {} - {}", new Object[]{s.size(),i,j});
		return s;
	}
	
	
}
