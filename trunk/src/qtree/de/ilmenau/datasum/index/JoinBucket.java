package de.ilmenau.datasum.index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Vector;

public class JoinBucket extends Bucket {
	private static final long serialVersionUID = 1L;
	private JoinSpace mySpace;
//	private double[] sourceCnt;
	private ArrayList<Double> sourceCnt;
	
	public JoinBucket(int[] minBounds, int[] maxBounds, double count, HashMap<String,Double> sourceIDMap,JoinSpace parentSpace) {
		super(minBounds,maxBounds,count,null,null,null);
		
		if (null != parentSpace) {
			mySpace = parentSpace;
			sourceIDCountMap = null;
			int init = sourceIDMap.size();
			if (parentSpace.getNrOfSources() > init) init = parentSpace.getNrOfSources();
			sourceCnt = new ArrayList<Double>(init);
			for (Map.Entry<String,Double> e : sourceIDMap.entrySet()) {
				int idx = parentSpace.addSource(e.getKey());
				while (sourceCnt.size() <= idx) sourceCnt.add(0.0);
				sourceCnt.set(idx,e.getValue());
			}
		}
		else {
			if (null == sourceIDCountMap) sourceIDCountMap = new HashMap<String, Double>();
			sourceIDCountMap.putAll(sourceIDMap);
		}
	}
	
	public void clear() {
		sourceCnt.clear();
		sourceCnt = null;
	}
	
	public JoinBucket(int[] minBounds, int[] maxBounds, double count, HashMap<String,Double> sourceIDMap) {
		this(minBounds,maxBounds,count,sourceIDMap,null);
	}
	
	public JoinBucket(int[] minBounds, int[] maxBounds, double count, HashSet<String> sourceIDs) {
		super(minBounds,maxBounds,count,null,null,-1,sourceIDs);
	}
	
	public double getSourceCnt(int idx) {
		if (sourceCnt.size() <= idx) return -1.0;
		return sourceCnt.get(idx);
	}
	
	public void updateCount(double amount) {
		
	}
	
	public void updateCount(double amount, String sourceID) {
		
	}

	public Vector<String> getAttributeNames() {
		return null;
	}
}
