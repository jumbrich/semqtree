package ie.deri.urq.realidy.index;

import ie.deri.urq.realidy.query.arq.QueryParser;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.vocabulary.RDF;

import de.ilmenau.datasum.exception.QTreeException;
import de.ilmenau.datasum.index.QueryResultEstimation;

public class SchemaIndex implements IndexInterface,Serializable {
	private final static Logger log = LoggerFactory.getLogger(InvertedURIIndex.class);
	
	Map<Resource, Set<String>> _propSource;
	Map<Resource, Set<String>> _typeSource;
	
	private QueryParser _qp = new QueryParser();
	
	public SchemaIndex() {
		_propSource = new HashMap<Resource, Set<String>>();
		_typeSource = new HashMap<Resource, Set<String>>();
	}
	
	public boolean addStatment(Node[] stmt) {
		if (stmt.length != 4) {
			return false;
		}
		
		Resource prop = (Resource)stmt[1];
		Set<String> pset = _propSource.get(prop);
		if (pset == null) {
			pset = new HashSet<String>();
		}
		pset.add(stmt[3].toString());
		_propSource.put(prop, pset);

		if (RDF.type.equals(stmt[1])) {
			Resource type = (Resource)stmt[2];
			Set<String> tset = _typeSource.get(type);
			if (tset == null) {
				tset = new HashSet<String>();
			}
			tset.add(stmt[3].toString());

			_typeSource.put(type, tset);
			return true;
		}
		return false;
	}
	
	public String getLabel(){
		return getVersionType();
	}
	
	public QueryResultEstimation evaluateQuery(String queryString)  throws Exception{
		Node[][] bgps = _qp.transform(queryString);
		
		QueryResultEstimation qre = new QueryResultEstimation(bgps.length);
		qre.setBGP(bgps);
		long start = System.currentTimeMillis();
		qre.setRelevantSourcesRanked(new ArrayList<String>(getRelevantSourcesForBgp(bgps)));
		qre.setTotalQueryTime(System.currentTimeMillis()-start);
		qre.setJoinIndices(_qp.findJoins(bgps));
		Integer [] order = new Integer [bgps.length	];
		for(int i =0; i < order.length; i++){
			order[i] =i;
		}
		qre.setJoinOrder(order);
		return qre;
	}
	
	public Collection<String> getRelevantSourcesForQuery(Op op) throws QTreeException {
		Node[][] bgps =_qp.transform(op);
		
		return getRelevantSourcesForBgp(bgps);
	}

	public Collection<String> getRelevantSourcesForQuery(String queryString) throws QTreeException {
		Node[][] bgps = _qp.transform(queryString);

		return getRelevantSourcesForBgp(bgps);
	}
	
	Collection<String> getRelevantSourcesForBgp(Node[][] bgps) {
		Set<String> sources = new HashSet<String>();
		
		for (Node[] bgp : bgps) {
			if (RDF.type.equals(bgp[1])) {
				if (_typeSource.containsKey((Resource)bgp[2])) {
					sources.addAll(_typeSource.get((Resource)bgp[2]));					
				}
			}
			
			if (_propSource.containsKey((Resource)bgp[1])) {
				sources.addAll(_propSource.get((Resource)bgp[1]));					
			}
		}
		return sources;
	}

	@Override
	public QueryResultEstimation evaluateQuery(String query, boolean reordering)
			throws Exception {
		return evaluateQuery(query);
	}


	@Override
	public String getVersionType() {
		return "schema";
	}

	@Override
	public boolean serialiseIndexToFile(File indexFile) {
		log.debug("Request for serialising a SemQtree to file {}.",indexFile.getAbsolutePath());

		long start = System.currentTimeMillis();

		if(indexFile.getParentFile()!=null && !indexFile.getParentFile().exists()){
			indexFile.getParentFile().mkdirs();
		}

		ObjectOutputStream oos;
		try {
			oos = new ObjectOutputStream(new FileOutputStream(indexFile));
			oos.writeObject(this);
			oos.close();
			
			long end = System.currentTimeMillis();

			log.info("Serialised the SemQtree in {} ms. on-disk size: {} KBytes to disk location {}\n",new Object[]{(end-start),indexFile.length()/1024} );
			return Boolean.TRUE;
		} catch (Exception e) {
			log.warn("During serialisation of SingleQTreeIndex", e);
		}
		return Boolean.FALSE;
	}

}
