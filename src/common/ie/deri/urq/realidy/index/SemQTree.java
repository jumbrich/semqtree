package ie.deri.urq.realidy.index;

import ie.deri.urq.realidy.hashing.HashingFactory;
import ie.deri.urq.realidy.hashing.QTreeHashing;
import ie.deri.urq.realidy.query.arq.QueryParser;
import ie.deri.urq.realidy.query.qtree.QueryProcessor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.sparql.algebra.Op;

import de.ilmenau.datasum.exception.QTreeException;
import de.ilmenau.datasum.index.AbstractIndex;
import de.ilmenau.datasum.index.QueryResultEstimation;
import de.ilmenau.datasum.index.SingleMultiDimHistogramIndex;
import de.ilmenau.datasum.index.SingleQTreeIndex;

public class SemQTree implements IndexInterface, Serializable{
	private static final long serialVersionUID = 1L;
	private final static Logger log = LoggerFactory.getLogger(SemQTree.class);

	private final boolean determineResultingBuckets = true;
	private final boolean useParentJoinSpace = false;

	private final String _hasherName;
	private final QTreeHashing _hasher;
	private final AbstractIndex _idx;
	private long _totalSerialisedTreeSize;
	private QueryParser _qp = new QueryParser();
	private QueryProcessor _qProc;
	private boolean _debug = false;

	/**
	 * Wrapper method to create an semqtreeindex with the specific settings
	 * @param hasher - name of the used hashing function -for a list of all possible hashing function see {@link HashingFactory}
	 * @param maxBuckets
	 * @param fanout
	 * @param dimMinValue
	 * @param dimMaxValue
	 * @param storeDetailedCounts
	 * @return
	 */
	public static SemQTree createSemQTreeSingleQTreeIndex(String hasher,int maxBuckets, int fanout, int dimMinValue,
			int dimMaxValue, boolean storeDetailedCounts){
		AbstractIndex idx = new SingleQTreeIndex(hasher, maxBuckets, fanout, dimMinValue, dimMaxValue, storeDetailedCounts);
		return new SemQTree(idx);
	}
	
	public static SemQTree createSemQTreeSingleQTreeIndex(QTreeHashing hashing,
			int maxBuckets, int fanout, int dimMinValue, int dimMaxValue,
			boolean storeDetailedCounts) {
		AbstractIndex idx = new SingleQTreeIndex(hashing.getHasherName(), maxBuckets, fanout, dimMinValue, dimMaxValue, storeDetailedCounts);
		return new SemQTree(idx,hashing);
	}
	public static SemQTree createSemQTreeSingleMultiDimHistogramIndex(String hasher,int maxBuckets, int fanout, int dimMinValue,
			int dimMaxValue, boolean storeDetailedCounts){
		AbstractIndex idx = new SingleMultiDimHistogramIndex(hasher, maxBuckets, dimMinValue, dimMaxValue, storeDetailedCounts);
		return new SemQTree(idx);
	}
	
	public static SemQTree createSemQTreeSingleMultiDimHistogramIndex(QTreeHashing hashing,
			int maxBuckets, int fanout, int dimMinValue, int dimMaxValue,
			boolean storeDetailedCounts) {
		AbstractIndex idx = new SingleMultiDimHistogramIndex(hashing.getHasherName(), maxBuckets, dimMinValue, dimMaxValue, storeDetailedCounts);
		return new SemQTree(idx,hashing);
	}
	/**
	 * @param hasher - name of the used hashing function -for a list of all possible hashing function see {@link HashingFactory}
	 * @param index
	 */
	public SemQTree(AbstractIndex  index) {
		this(index,HashingFactory.createHasher(index.getHasher(), index.getDimSpecMin(), index.getDimSpecMax()));
	}

	public SemQTree(AbstractIndex idx, QTreeHashing hashing) {
		_idx = idx;
		_hasherName = idx.getHasher();
		_hasher=hashing;
		_qProc = new QueryProcessor(_idx);
		log.info("[INIT] SemQTree with {} hasher",_hasherName);
	}
	public boolean addStatment(final Node[] stmt) {
		int[] hashCoordinates = _hasher.getHashCoordinates(Arrays.copyOf(stmt, 3));
//		System.out.println(Nodes.toN3(stmt)+" =>> "+Arrays.toString(hashCoordinates));
		return _idx.insertStatement(hashCoordinates, stmt[stmt.length-1].toString());
	}

	/**
	 * prints an detailed info about the configuration and content of the index
	 * @return - a String object
	 */
	public String info(){
		StringBuilder sb = new StringBuilder("[");
		sb.append(this.getClass().getSimpleName()).append("]");
		sb.append("\n");
		sb.append("  index:   ").append(_idx.getClass().getSimpleName()).append("\n");
		sb.append("  hasher:   ").append(_hasherName).append("\n");
		sb.append(_idx.info());

		return sb.toString();
	}

	public boolean serialiseIndexToFile(File indexFile) {
		log.debug("Request for serialising a SemQtree to file {}.",indexFile.getAbsolutePath());

		long start = System.currentTimeMillis();

		if(indexFile.getParentFile()!=null && !indexFile.getParentFile().exists()){
			indexFile.getParentFile().mkdirs();
		}

		ObjectOutputStream oos;
		try {
			oos = new ObjectOutputStream(new FileOutputStream(indexFile));
			oos.writeObject(_idx);
			oos.close();
			_totalSerialisedTreeSize += indexFile.length();
			long end = System.currentTimeMillis();

			log.info("Serialised the SemQtree in {} ms. on-disk size: {} KBytes to disk location {}\n{}",new Object[]{(end-start),indexFile.length()/1024 ,indexFile,info()} );
			return Boolean.TRUE;
		} catch (Exception e) {
			log.warn("During serialisation of SingleQTreeIndex", e);
		}
		return Boolean.FALSE;

	}

	public static SemQTree loadIndex(File indexFile) {

		log.info("Request for deserialising a from file {}.",indexFile.getAbsolutePath());
		ObjectInputStream ois;
		try {
			long start= System.currentTimeMillis();
			ois = new ObjectInputStream(new FileInputStream(indexFile));
			AbstractIndex idx = (AbstractIndex)  ois.readObject();

			SemQTree sqt = new SemQTree(idx);
			log.info("Loaded index from location {} with size {} KBytes in {} ms.", new Object[]{indexFile,indexFile.length()/1024,(System.currentTimeMillis()-start)});
			return sqt;
		} catch (Exception e) {
			log.warn("Request for deserialisation of SingleQTreeIndex failed", e);
			return null;
		}
	}
	

	public static SemQTree loadIndex(File indexFile, QTreeHashing hashing) {
		log.debug("Request for deserialising a from file {}.",indexFile.getAbsolutePath());
		ObjectInputStream ois;
		try {
			long start= System.currentTimeMillis();
			ois = new ObjectInputStream(new FileInputStream(indexFile));
			AbstractIndex idx = (AbstractIndex)  ois.readObject();

			SemQTree sqt = new SemQTree(idx,hashing);
			log.info("Loaded index from location {} with size {} KBytes in {} ms.", new Object[]{indexFile,indexFile.length()/1024,(System.currentTimeMillis()-start)});
			return sqt;
		} catch (Exception e) {
			log.warn("Request for deserialisation of SingleQTreeIndex failed", e);
			return null;
		}

	}


	public int getNoOfStmts() {
		return _idx.getNoOfStmts();
	}


	public Collection<String> getRelevantSourcesForQuery(Op op) throws QTreeException {
		return evaluateQuery(op).getRelevantSourcesRanked();
	}



	public Collection<String> getRelevantSourcesForQuery(String queryString) throws QTreeException {
		return evaluateQuery(queryString).getRelevantSourcesRanked();
	}

	public Collection<String> getRelevantSourcesForQuery(String queryString, boolean determineResultingBuckets, boolean useParentJoinSpace) throws QTreeException {
		
		return evaluateQuery(queryString,determineResultingBuckets,useParentJoinSpace,true).getRelevantSourcesRanked();
	}

	public QueryResultEstimation evaluateQuery(String queryString, boolean reordering)  throws Exception{
		return evaluateQuery(queryString, determineResultingBuckets, useParentJoinSpace,reordering);
	}
	
	public QueryResultEstimation evaluateQuery(String queryString) throws QTreeException{
		return evaluateQuery(queryString, determineResultingBuckets, useParentJoinSpace,true);
	}
	
	public QueryResultEstimation evaluateQuery(Op op) throws QTreeException {
		Node[][] bgps =_qp.transform(op);
		return evaluateQuery( bgps,determineResultingBuckets,useParentJoinSpace,true);
	}
	
	public QueryResultEstimation evaluateQuery(String queryString,
			boolean determineResultingBuckets, boolean useParentJoinSpace, boolean reordering) throws QTreeException {
		Node[][] bgps = _qp.transform(queryString);
		return evaluateQuery(bgps, determineResultingBuckets, useParentJoinSpace,reordering);
	}
	
	
	private QueryResultEstimation evaluateQuery(Node[][] bgps, boolean determineResultingBuckets, boolean useParentJoinSpace, boolean reordering) throws QTreeException {
		int [][] hasCoordinates = new int[bgps.length][];
		for(int i = 0; i < bgps.length; i++){
			hasCoordinates[i] = _hasher.getHashCoordinates(bgps[i]); 
			System.out.println(Nodes.toN3(bgps[i])+" =>> "+Arrays.toString(hasCoordinates[i]));
		}
		if(_qProc!= null ) return _qProc.executeQuery(bgps, hasCoordinates, determineResultingBuckets, reordering);
		else return _idx.evaluateQuery(hasCoordinates,_qp.findJoins(bgps), determineResultingBuckets, useParentJoinSpace);
	}

	public int getNoOfSrc() {
		return _idx.getNoOfSources();
	}

	public String getConfigurationLabel() {
		return null;
	}

	public AbstractIndex getAbstractIndex() {
		return _idx;
	}

	public void enableDebugMode(boolean enableDebug) {
		log.info("Enabling debug mode: {}",enableDebug);
		
		_debug  = enableDebug;
		getAbstractIndex().enableDebugMode(enableDebug);
		_qProc.enableDebugMode(enableDebug);
	}

	public String getVersionType() {
		return _idx.getVersionType(); 
	}

	public QTreeHashing getHasher() {
		return _hasher;
	}

	public void setInvBucketOperator(boolean b) {
		if(_qProc!= null ) _qProc.setInvBucketOperator(b);
		
	}

	
	public String getLabel(){
		return getVersionType()+"-"+getHasher().getHasherName();
	}
	

}