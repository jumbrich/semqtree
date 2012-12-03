package ie.deri.urq.realidy.query.arq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.semanticweb.yars.nx.Node;

import de.ilmenau.datasum.index.QueryResultEstimation;

public class QueryBenchmark {

    
    private String _query;
    private final Map<String, TimePair> _measures = new HashMap<String, TimePair>();
    private List<QueryResultEstimation> _queryResultEstimations = new ArrayList<QueryResultEstimation>();
    private final Set<Node> _matchSrc = new java.util.HashSet<Node>();
    
    public void setQuery(String query){
	_query = query;
    }

    public void setTime(String string, long currentTimeMillis, boolean b) {
	TimePair p = _measures.get(string);
	if(p == null) p = new TimePair();
	p.setTime(currentTimeMillis,b);
	_measures.put(string,p);
//	System.out.println("\n====== setTime for "+string+" map:"+_measures.size());
    }

    public void setQueryResultEstimation(QueryResultEstimation qre) {
	_queryResultEstimations.add(qre);
	
    }

    public void addSrc(Node node) {
	_matchSrc.add(node);
    }
    
    
    @Override
    public String toString() {
	StringBuilder sb = new StringBuilder("=== QueryBenchmark ===\n");
	if(_query!=null)sb.append("|-\nQuery:/n").append(_query).append("\n-|");
	sb.append("\n\nContributedSources: ").append(_matchSrc.size());
	sb.append("\nElapsed Time:\n");
	
	for(String s: _measures.keySet()){
	    sb.append("  ").append(s).append("\t").append(_measures.get(s).getDifference()).append(" ms!\n");
	}
	sb.append("\n");
	for(QueryResultEstimation qre: _queryResultEstimations){
	    sb.append(qre.toString()).append("\n\n");
	}
	
	return sb.toString();
    }
}
class TimePair{
    long[] t = new long[2];
    
    public void setStart(long time){
	t[0]=time;
    }
    public long getDifference() {
	return t[1]-t[0];
    }
    public void setEnd(long time){
	t[1]=time;
    }
    public void setTime(long time, boolean isStart){
	if(isStart) setStart(time);
	else setEnd(time);
    }
	
}
