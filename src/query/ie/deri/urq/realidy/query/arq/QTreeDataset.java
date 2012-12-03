package ie.deri.urq.realidy.query.arq;

import ie.deri.urq.realidy.index.SemQTree;

import com.hp.hpl.jena.sparql.core.DataSourceGraphImpl;
import com.hp.hpl.jena.sparql.core.DatasetImpl;

public class QTreeDataset extends DatasetImpl {

    public QTreeDataset(SemQTree sqt, boolean enableDLApproach, int threads,  int topK, QueryBenchmark bench) {
	super(new QTreeDataSourceGraph(sqt,enableDLApproach,threads,topK,bench));
    }
}



class QTreeDataSourceGraph extends DataSourceGraphImpl{
    private int _threads;
    private QueryBenchmark _bench;
    private SemQTree _sqt;
    private int _topK;
	private boolean _enableDLApproach;

    public QTreeDataSourceGraph(SemQTree sqt, boolean enableDLApproach, int threads,  int topK, QueryBenchmark bench) {
	super();
	_threads = threads;
	_bench = bench;
	_sqt = sqt;
	_topK = topK;
		_enableDLApproach = enableDLApproach;
    }

    public int getThreads() {
	return _threads;
    }

    public QueryBenchmark getBenchmark(){
	return _bench;
    }

    public SemQTree getSemQTree() {
	return _sqt;
    }
    
    public int getTopK(){
	return _topK;
    }
    public boolean enableDLApproach(){return _enableDLApproach;}
}
