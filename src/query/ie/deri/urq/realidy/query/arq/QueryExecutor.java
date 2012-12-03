package ie.deri.urq.realidy.query.arq;

import ie.deri.urq.realidy.index.SemQTree;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;

public class QueryExecutor {

	private final SemQTree _sqt;
	private int threads = 50;
	private int topK = 400;
	private boolean _enableDLApproach;

	public QueryExecutor(SemQTree sqt, boolean enableDLApproach) {
		_sqt = sqt;
		_enableDLApproach = enableDLApproach;
		QTreeQueryEngine.register();
	}

	public QueryBenchmark benchmarkQuery(String queryString, int threads, int topK){ 
		QueryBenchmark bench = new QueryBenchmark();
		executeQuery(queryString, threads, topK, bench);
		return bench;
	}

	public QueryBenchmark benchmarkQuery(String queryString){
		QueryBenchmark bench = new QueryBenchmark();
		executeQuery(queryString, threads, topK, null);
		return bench;
	}

	public ResultSet executeQuery(String queryString){
		return executeQuery(queryString, threads, topK, null);
	}

	public ResultSet executeQuery(String queryString, int threads, int topK, QueryBenchmark bench){
		Query query = QueryFactory.create(queryString);
		QTreeDataset ds =  new QTreeDataset(_sqt,_enableDLApproach,threads,topK, bench);
		QueryExecution engine = QueryExecutionFactory.create(query,ds);
		ResultSet results = engine.execSelect() ;

		return results;
	}
}