package de.ilmenau.datasum.index;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;


import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.slf4j.LoggerFactory;

/**
 * This class encapsulates the result of a query estimation and some measurements.
 * @author MKa
 *
 */
public class QueryResultEstimation implements Serializable{
	private final static org.slf4j.Logger log = LoggerFactory.getLogger(QueryResultEstimation.class);
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private long[] bgpEvalTimes;
	private long[] bgpEstSources;
	private int[] bgpResultingBuckets;
	private long[] bgpQTreeBuildTimes;
	private long[] joinEvalTimes;
	private int[] joinResultingBuckets;
	private ArrayList<String> relevantSourcesRanked;
	private ArrayList<String> relevantSourcesRankedAdvanced;
	private long rankTime;
	private long advancedRankTime;
	private long totalQuerytime;
	private boolean _debug;
	private String _queryString;
	private int[][] _joinIndices;
	private Integer[] _joinOrder;
	private int[][] _oldJoinIndices;
	private long _orderingTime;
	private Node[][] _bgp;
	private String _op;
	private Map<Long,String> _indexEvalMap = new TreeMap<Long, String>();
	private Integer[] _oldJoinOrder;

	public QueryResultEstimation(int nrBgps) {
		this(nrBgps,false);
	}


	public QueryResultEstimation(int nrBgps, boolean debug) {
		bgpEvalTimes = new long[nrBgps];
		bgpEstSources = new long[nrBgps];
		bgpResultingBuckets = new int[nrBgps];
		bgpQTreeBuildTimes = new long[nrBgps];
		joinEvalTimes = new long[nrBgps-1];
		joinResultingBuckets = new int[nrBgps-1];
		
		Arrays.fill(bgpEvalTimes,-1L);
		Arrays.fill(bgpEstSources,-1L);
		Arrays.fill(bgpResultingBuckets,-1);
		Arrays.fill(bgpQTreeBuildTimes,-1L);
		Arrays.fill(joinEvalTimes,-1L);
		Arrays.fill(joinResultingBuckets,-1);
		
		totalQuerytime = 0L;
		_debug = debug;
	}


	public Long getTotalQueryTime(){
		return totalQuerytime;
	}


	public void setTotalQueryTime(long time){
		if(_debug) log.debug("Total query time: {} ms",time);
		totalQuerytime = time;
	}
	/**
	 * the query times for the single BGPs
	 * @return
	 */
	public long[] getBgpEvalTimes() {
		return bgpEvalTimes;
	}

	public void setBgpEvalTime(long time, int index) {
		if(_debug) log.debug("  BGP-{} eval time: {} ms", new Object[]{index,time});
		this.bgpEvalTimes[index] = time;
	}

	/**
	 * number of estimated sources for the single BGPs
	 * @return
	 */
	public long[] getBgpEstSources() {
		return bgpEstSources;
	}

	public void setBgpEstSources(long[] bgpEstSources) {
		this.bgpEstSources = bgpEstSources;
	}

	public void setBgpEstSources(long time, int index) {
		if(_debug)log.debug("  BGP-{} results in {} sources",new Object[]{index,time});
		this.bgpEstSources[index] = time;
	}

	/**
	 * the number of buckets that the resulting QTree after each BGP evaluation contains
	 * @return
	 */
	public int[] getBgpResultingBuckets() {
		return bgpResultingBuckets;
	}

	public void setBgpResultingBuckets(int[] bgpResultingBuckets) {
		this.bgpResultingBuckets = bgpResultingBuckets;
	}

	public void setBgpResultingBuckets(int buckets, int index) {
		if(_debug)log.debug("  BGP-{} number of buckets: {} ",new Object[]{index,buckets});
		this.bgpResultingBuckets[index] = buckets;
	}

	/**
	 * the times to build the resulting QTree after each BGP evaluation
	 * @return
	 */
	public long[] getBgpQTreeBuildTimes() {
		return bgpQTreeBuildTimes;
	}

	public void setBgpQTreeBuildTimes(long[] bgpQTreeBuildTimes) {
		
		this.bgpQTreeBuildTimes = bgpQTreeBuildTimes;
	}

	public void setBgpQTreeBuildTime(long time, int index) {
		this.bgpQTreeBuildTimes[index] = time;
	}

	/**
	 * query times for the single joins - the last one is the final join
	 * @return
	 */
	public long[] getJoinEvalTimes() {
		return joinEvalTimes;
	}

	public void setJoinEvalTimes(long[] joinEvalTimes) {
		this.joinEvalTimes = joinEvalTimes;
	}

	public void setJoinEvalTime(long time, int index) {
		if(_debug)log.debug("  JOIN-{} join time {} ms ",new Object[]{index,time});
		this.joinEvalTimes[index] = time;
	}

	/**
	 * the number of buckets that the resulting QTree after each join evaluation contains
	 * @return
	 */
	public int[] getJoinResultingBuckets() {
		return joinResultingBuckets;
	}

	public void setJoinResultingBuckets(int[] joinResultingBuckets) {
		this.joinResultingBuckets = joinResultingBuckets;
	}

	public void setJoinResultingBuckets(int buckets, int index) {
		if(_debug)log.debug("  JOIN-{} resulting buckets {} ", new Object[]{index,buckets});
		this.joinResultingBuckets[index] = buckets;
	}

	/**
	 * the relevant sources ranked - the first one is the highest ranked
	 * @return
	 */
	public ArrayList<String> getRelevantSourcesRanked() {
		return relevantSourcesRanked;
	}

	public void setRelevantSourcesRanked(ArrayList<String> relevantSourcesRanked) {
		this.relevantSourcesRanked = relevantSourcesRanked;
	}

	/**
	 * the relevant sources ranked with another method - the first one is the highest ranked
	 * @return
	 */
	public ArrayList<String> getRelevantSourcesRankedAdvanced() {
		return relevantSourcesRankedAdvanced;
	}

	public void setRelevantSourcesRankedAdvanced(
			ArrayList<String> relevantSourcesRankedAdvanced) {
		this.relevantSourcesRankedAdvanced = relevantSourcesRankedAdvanced;
	}

	/**
	 * time needed to determine the standard ranking
	 * @return
	 */
	public long getRankTime() {
		return rankTime;
	}

	public void setRankTime(long rankTime) {
		this.rankTime = rankTime;
	}

	/**
	 * time needed to determine the advanced ranking
	 * @return
	 */
	public long getAdvancedRankTime() {
		return advancedRankTime;
	}

	public void setAdvancedRankTime(long advancedRankTime) {
		this.advancedRankTime = advancedRankTime;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder(this.getClass().getSimpleName());
		sb.append("\n--").append(this.getClass().getSimpleName()).append("\n--");
		if(_queryString!=null) sb.append("\n>Query\n").append(_queryString).append("<Query");
		sb.append("\nJoin.Ordering: ").append(_orderingTime).append(" (ms)");
		if(_joinOrder!=null)sb.append("\n>Orig.Join.Order:\n").append(Arrays.toString(_oldJoinOrder));
		if(_oldJoinIndices!=null){
			sb.append("\nInit.Join.Positions:\n");
			for(int[] i : _oldJoinIndices){
				sb.append(Arrays.toString(i)).append("\n");
			}
		}
		if(_joinOrder!=null)sb.append("\n>Opt.Join.Order:\n").append(Arrays.toString(_joinOrder));
		if(_joinIndices!=null){
			sb.append("\nOpt.Join.Positions:\n");
			for(int[] i : _joinIndices){
				sb.append(Arrays.toString(i)).append("\n");
			}
			for(int i = 0; i < _joinOrder.length;i++){
				sb.append("\n ").append(Nodes.toN3(_bgp[_joinOrder[i]]));
			}
		}
		sb.append("\n>Operator: ").append(_op);
		sb.append("\n--\nTOTAL_QUERY_TIME: ").append(totalQuerytime).append(" ms");
		sb.append("\n BGP");
		sb.append("\n  EVAL_TIME (BGP):");
		for(int i = 0; i < bgpEvalTimes.length; i++){
			sb.append("\n   BGP-").append(i).append(": ").append(bgpEvalTimes[i]).append(" ms");
		}
		sb.append("\n  EST_SOURCES (BGP):");
		for(int i = 0; i < bgpEstSources.length; i++){
			sb.append("\n   BGP-").append(i).append(": ").append(bgpEstSources[i]).append(" sources");
		}
		sb.append("\n  RES_BUCKETS (BGP):");
		for(int i = 0; i < bgpResultingBuckets.length; i++){
			sb.append("\n   BGP-").append(i).append(": ").append(bgpResultingBuckets[i]).append(" buckets");
		}
		sb.append("\n JOIN");
		sb.append("\n  JOIN_EVAL_TIME:");
		for(int i = 0; i < joinEvalTimes.length; i++){
			sb.append("\n   JOIN-").append(i).append(": ").append(joinEvalTimes[i]).append(" ms");
		}
		sb.append("\n  JOIN_RESULTING_BUCKETS:");
		for(int i = 0; i < joinResultingBuckets.length; i++){
			sb.append("\n   JOIN-").append(i).append(": ").append(joinResultingBuckets[i]).append(" buckets");
		}
		sb.append("\n RANKING");
		if(relevantSourcesRanked!=null){
			sb.append("\n  TIME_TO_RANK: ").append(rankTime).append(" ms");
			sb.append("\n  NO_RANKED_SRCS: ").append(relevantSourcesRanked.size());
		}if(relevantSourcesRankedAdvanced !=null){
			sb.append("\n  TIME_TO_RANK_ADV: ").append(advancedRankTime).append(" ms");
			sb.append("\n  NO_RANK_ADV_SRCS: ").append(relevantSourcesRankedAdvanced.size());
		}
		sb.append("\n--");
		return sb.toString();
	}

	public void setQueryString(String query) {
		_queryString = query;
	}
	
	public String getQueryString(){
		return _queryString;
	}

	public void setJoinOrder(Integer[] newJoinOrder) {
		_joinOrder = newJoinOrder;
	}
	
	public void setOldJoinOrder(Integer[] newJoinOrder) {
		_oldJoinOrder = newJoinOrder;
	}
	
	public Integer[] getJoinOrder(){
		return _joinOrder;
	}

	public void setJoinIndices(int[][] joinIndices) {
		_joinIndices = joinIndices;	
	}

	public int[][] getJoinIndices(){
		return _joinIndices;
	}

	public void setOldIndices(int[][] joinIndices) {
		_oldJoinIndices = joinIndices;
	}

	public void setOrderingTime(long l) {
		_orderingTime = l;
	}

	public void setBGP(Node[][] bgps) {
		_bgp = bgps;
	}

	public void setOperator(String op) {
		_op = op;
	}

	public void setIndexEval(String string) {
		_indexEvalMap .put(System.currentTimeMillis(),string);
	}
}
