package ie.deri.urq.realidy.bench.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.math.stat.descriptive.SummaryStatistics;


public class NXBenchSummary {

	public static final int TOTAL_QUERY_TIME = 0;
	public static final int JOIN_TIME = 1;
	public static final int RANK_TIME = 2;
	public static final int BUCKETS = 3;
	public static final int REAL_SOURCES = 4;
	public static final int EST_SOURCES = 5;
	public static final int REAL_STMS = 6;
	public static final int REAL_STMS_TIME = 7;
	public static final int QTREE_STMTS = 8;
	public static final int QTREE_STMTS_PER = 9;
	public static final int QTREE_STMTS_TIME = 10;
	public static final int TOP_10_STMTS = 11;
	public static final int TOP_10_STMTS_PER = 12;
	public static final int TOP_10_STMTS_TIME = 13;
	public static final int TOP_50_STMTS = 14;
	public static final int TOP_50_STMTS_PER = 15;
	public static final int TOP_50_STMTS_TIME = 16;
	public static final int TOP_100_STMTS = 17;
	public static final int TOP_100_STMTS_PER = 18;
	public static final int TOP_100_STMTS_TIME = 19;
	public static final int TOP_200_STMTS = 20;
	public static final int TOP_200_STMTS_PER = 21;
	public static final int TOP_200_STMTS_TIME = 22;
	public static final int MAX_K = 23;
	
	
	
//	stats[]
	
	public final static Map<Integer,String> labels = new HashMap<Integer, String>();
	static{
		labels.put(TOTAL_QUERY_TIME, "TOTAL_QURY_TIME");
		labels.put(JOIN_TIME,"JOIN_TIME");
		labels.put(RANK_TIME,"RANK_TIME");
		labels.put(BUCKETS,"BUCKETS");
		labels.put(REAL_SOURCES,"REAL_SOURCES");
		labels.put(EST_SOURCES,"EST_SOURCES");
		labels.put(REAL_STMS,"REAL_STMS");
		labels.put(REAL_STMS_TIME,"REAL_STMS_TIME");
		labels.put(QTREE_STMTS,"QTREE_STMTS");
		labels.put(QTREE_STMTS_PER,"QTREE_STMTS_PER");
		labels.put(QTREE_STMTS_TIME,"QTREE_STMTS_TIME");
		labels.put(TOP_10_STMTS,"TOP_10_STMTS");
		labels.put(TOP_10_STMTS_PER,"TOP_10_STMTS_PER");
		labels.put(TOP_10_STMTS_TIME,"TOP_10_STMTS_TIME");
		labels.put(TOP_50_STMTS,"TOP_50_STMTS");
		labels.put(TOP_50_STMTS_PER,"TOP_50_STMTS_PER");
		labels.put(TOP_50_STMTS_TIME,"TOP_50_STMTS_TIME");
		labels.put(TOP_100_STMTS,"TOP_100_STMTS");
		labels.put(TOP_100_STMTS_PER,"TOP_100_STMTS_PER");
		labels.put(TOP_100_STMTS_TIME,"TOP_100_STMTS_TIME");
		labels.put(TOP_200_STMTS,"TOP_200_STMTS");
		labels.put(TOP_200_STMTS_PER,"TOP_200_STMTS_PER");
		labels.put(TOP_200_STMTS_TIME,"TOP_200_STMTS_TIME");
		labels.put(MAX_K,"MAX_K");
	}
	SummaryStatistics[] stats = new SummaryStatistics[labels.size()];
	  public NXBenchSummary() {
		 for( int i =0; i < stats.length;i++){
			 stats[i] = new SummaryStatistics(); 
		 }
	}    
	      
	public void process(File queryBench) {
		Scanner s;
		try {
			s = new Scanner(queryBench);
			s.nextLine();
			while(s.hasNextLine()){
				String[] split = s.nextLine().split(" ");
				for( int i =0; i < stats.length;i++){
					Double v = Double.valueOf(split[i+1]);
					if(!v.isNaN()){
						stats[i].addValue(v);
					}
					else{
						System.out.println(queryBench.getAbsolutePath()+" skip non readable line: "+Arrays.toString(split));
					}
				 }
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public long getCount(int i){
		return stats[i].getN();
	}
	
	public double getMean(int i){
		return NaNCheck(stats[i].getMean());
	}
	
	
	public String toString(Integer statsID) {
		StringBuilder sb = new StringBuilder();
		sb.append(getCount(statsID));
		sb.append(" ").append(getMin(statsID));
		sb.append(" ").append(getMean(statsID));
		sb.append(" ").append(getMax(statsID));
		sb.append(" ").append(getStdDev(statsID));
		
		return sb.toString();
	}

	private double getStdDev(Integer statsID) {
		return NaNCheck(stats[statsID].getStandardDeviation());
	}

	private double NaNCheck(double value) {
		if( value == Double.NaN ) return -1D;
		else return value;
	}

	private double getMax(Integer statsID) {
		return NaNCheck(stats[statsID].getMax());
	}

	private double getMin(Integer statsID) {
		return NaNCheck(stats[statsID].getMin());
	}
}
