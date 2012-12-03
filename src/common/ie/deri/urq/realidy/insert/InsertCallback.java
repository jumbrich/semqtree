package ie.deri.urq.realidy.insert;

import ie.deri.urq.realidy.index.IndexInterface;
import ie.deri.urq.realidy.index.SemQTree;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.ilmenau.datasum.index.AbstractIndex;

public class InsertCallback implements Callback {
	private final static Logger log = LoggerFactory.getLogger(InsertCallback.class);
	private IndexInterface _index;
	public InsertCallback(IndexInterface index) {
		_index = index;
	}

	public void endDocument() {;}

	public void processStatement(Node[] stmt) {
		_index.addStatment(stmt);
	}

	public void startDocument() {}
}
