package ie.deri.urq.realidy.bench.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.ilmenau.datasum.index.QueryResultEstimation;

public class JoinOrderBenchSummary {
	private final static Logger log = LoggerFactory.getLogger(JoinOrderBenchSummary.class);
	public static final int TOTAL_QUERY_TIME = 0;
	public static final int BUCKETS = 1;
	
	private String index,queryFile;
	
//	stats[]
	 private boolean _noOpt = false;
	private boolean _invIdx;
	
	SummaryStatistics[] stats = new SummaryStatistics[2];
	public final static Map<Integer,String> labels = new HashMap<Integer, String>();
	static{
		labels.put(TOTAL_QUERY_TIME, "TOTAL_QURY_TIME");
		labels.put(BUCKETS,"BUCKETS");
	}
	
	  public JoinOrderBenchSummary() {
		 for( int i =0; i < stats.length;i++){
			 stats[i] = new SummaryStatistics(); 
		 }
	}    
	      
	public JoinOrderBenchSummary(File queryBench) {
		this();
		String filename  = queryBench.getName();
		if(queryBench.getName().startsWith("query-no-opt_")){
			index = filename.substring(13,filename.indexOf(".qre"));
			_noOpt = true;
		}else if(queryBench.getName().startsWith("query-inv_idx_")) {
			index = filename.substring(14,filename.indexOf(".qre"));
			_invIdx = true;
		}else{
			index = filename.substring(6,filename.indexOf(".qre"));
		}
		queryFile = queryBench.getParentFile().getName();
	}

	public void process(File queryBench) {
		QueryResultEstimation rqt = deserialise(queryBench);
		
		stats[TOTAL_QUERY_TIME].addValue(rqt.getTotalQueryTime()); 
			
		if(rqt.getJoinResultingBuckets().length == 0){
			stats[BUCKETS].addValue(rqt.getBgpResultingBuckets()[0]);	
		}
		else{
			stats[BUCKETS].addValue(rqt.getJoinResultingBuckets()[rqt.getJoinResultingBuckets().length-1]);
		}
	}
	
	private QueryResultEstimation deserialise(File queryBench) {
		ObjectInputStream ois;
		QueryResultEstimation res=null;
		try {
			ois = new ObjectInputStream(new FileInputStream(queryBench));
			res = (QueryResultEstimation)ois.readObject();
			ois.close();
			log.info("Deserialised QueryResultEstimation {}",queryBench.getAbsolutePath());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return res;
	}

	
	public long getCount(int i){
		return stats[i].getN();
	}
	
	public double getMean(int i){
		return NaNCheck(stats[i].getMean());
	}
	
	public String getIndex(){return index;}
	public String getQuery(){return queryFile;}

	public String indexString(Integer statsID) {
		return toString(getIndex(), statsID);
	}

	private String toString(String index, Integer statsID) {
		StringBuilder sb = new StringBuilder(index);
		sb.append(" ").append(getCount(statsID));
		sb.append(" ").append(getMin(statsID));
		sb.append(" ").append(getMean(statsID));
		sb.append(" ").append(getMax(statsID));
		sb.append(" ").append(getStdDev(statsID));
		
		return sb.toString();
	}

	private double getStdDev(Integer statsID) {
		return NaNCheck(stats[statsID].getStandardDeviation());
	}

	private double getMax(Integer statsID) {
		return NaNCheck(stats[statsID].getMax());
	}

	private double getMin(Integer statsID) {
		return NaNCheck(stats[statsID].getMin());
	}

	public String queryString(Integer statsID) {
		return toString(getQuery(), statsID);
	}

	public boolean noOpt() {
		return _noOpt;	
	}
	
	public boolean invIdx(){
		return _invIdx;
	}
	private double NaNCheck(double value) {
		if( value == Double.NaN ) return -1D;
		else return value;
	}
	
	@Override
	public String toString() {
		return "JoinOrderBenchSummary: "+index+" q:"+queryFile+" opt:"+noOpt()+" inv:"+invIdx();
	}
}