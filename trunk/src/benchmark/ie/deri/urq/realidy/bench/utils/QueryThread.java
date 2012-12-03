package ie.deri.urq.realidy.bench.utils;

import ie.deri.urq.realidy.index.IndexInterface;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.ilmenau.datasum.index.QueryResultEstimation;

public class QueryThread extends Thread{
	private final static Logger log = LoggerFactory.getLogger(QueryThread.class);
	private Long _elapsed = null;
	private String _query;
	private IndexInterface _sqt;
	
	private QueryResultEstimation qre = new QueryResultEstimation(1);
	private boolean _reordering;
	private boolean finished = false;
	public QueryThread(IndexInterface sqt,String query) {
		this(sqt,query,true);
	}
	public QueryThread(IndexInterface sqt, String query, boolean reordering) {
		_sqt = sqt;
		_query = query;
		_reordering = reordering;
	}
	public void run() {
		long start = System.currentTimeMillis();
		try {
			qre = _sqt.evaluateQuery(_query,_reordering);
			if(qre!=null)
				qre.setQueryString(_query);
			
		} catch (Exception e) {
			e.printStackTrace();
			finished = true;
		}
		_elapsed = System.currentTimeMillis()-start;
		finished = true;
		log.info("finished for query, result:"+(qre!=null));
	}
	public boolean finished() {
//		qre=null;
		return finished;
	}
	public QueryResultEstimation getQueryResultEstimation() {
		log.info("get for query, result:"+(qre!=null));
		return qre ;
	}
	public String getElapsedTime() {
		if(_elapsed==null) return "-1";
		return _elapsed.toString();
	}
}
