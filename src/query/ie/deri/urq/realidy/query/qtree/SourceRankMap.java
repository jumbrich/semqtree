package ie.deri.urq.realidy.query.qtree;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;

public class SourceRankMap {

	private TreeMap<Double, Set<String>> _internalMap =null;
	private HashMap<String, Double> _srcRankMap;

	public SourceRankMap() {
//		_internalMap = new TreeMap()<Double, List<String>>();
		_srcRankMap = new HashMap<String, Double>();
	}
	
	public Double get(String source) {
		return _srcRankMap.get(source);
	}

	public void put(String source, Double cnt) {
		_srcRankMap.put(source,cnt);
	}

	public TreeMap<Double, Set<String>> getRankSourceMap() {
		if(_internalMap==null){
			_internalMap = new TreeMap<Double, Set<String>>();
			for(Entry<String, Double> ent: _srcRankMap.entrySet()){
				Set<String> l = _internalMap.get(ent.getValue());
				if(l== null) l= new HashSet<String>();
				l.add(ent.getKey());
//				
				_internalMap.put(ent.getValue(),l);
			}
		}
		return _internalMap;
	}
}