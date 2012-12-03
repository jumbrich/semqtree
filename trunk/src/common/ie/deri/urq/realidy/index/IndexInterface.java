 package ie.deri.urq.realidy.index;

import java.io.File;
import java.util.Collection;

import org.semanticweb.yars.nx.Node;

import com.hp.hpl.jena.sparql.algebra.Op;

import de.ilmenau.datasum.index.QueryResultEstimation;

public interface IndexInterface {
	public boolean addStatment(final Node[] stmt);
	public Collection<String> getRelevantSourcesForQuery(Op op) throws Exception;
	public Collection<String> getRelevantSourcesForQuery(String queryString) throws Exception;
	public QueryResultEstimation evaluateQuery(String queryString)  throws Exception;
	public QueryResultEstimation evaluateQuery(String query, boolean reordering)  throws Exception;
	public String getVersionType();
	public String getLabel();
	public boolean serialiseIndexToFile(File indexFile);
}
