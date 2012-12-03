package ie.deri.urq.realidy.query.arq;

import java.util.HashSet;
import java.util.Set;

import com.hp.hpl.jena.sparql.algebra.OpVisitor;
import com.hp.hpl.jena.sparql.core.BasicPattern;

import de.ilmenau.datasum.index.AbstractIndex;

public abstract class QTreeOpVisitor implements OpVisitor {

    protected HashSet<String> sources;
    protected AbstractIndex _index;
    protected BasicPattern _pattern;
    protected QueryBenchmark _bench;
    protected int _topK;

    public QTreeOpVisitor(AbstractIndex index) {
	this(index,-1,null);
    }

    public QTreeOpVisitor(AbstractIndex index, int topK, QueryBenchmark bench) {
	sources = new HashSet<String>();
	_bench = bench;
	_index = index;
	_topK = topK;
    }


    public Set<String> getSources(){
	return sources;
    }

    public BasicPattern getPattern(){
	return _pattern;
    }
    
    public AbstractIndex getIndex(){
	return _index;
    }
    
    public QueryBenchmark getBenchmark(){
	return _bench;
    }
    
    public Integer getTopK(){
	return _topK;
    }
}
