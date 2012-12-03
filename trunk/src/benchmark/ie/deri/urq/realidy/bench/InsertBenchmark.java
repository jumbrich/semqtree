package ie.deri.urq.realidy.bench;
import ie.deri.urq.realidy.bench.utils.BenchmarkConfig;
import ie.deri.urq.realidy.bench.utils.Printer;
import ie.deri.urq.realidy.hashing.QTreeHashing;
import ie.deri.urq.realidy.index.IndexFactory;
import ie.deri.urq.realidy.index.IndexInterface;
import ie.deri.urq.realidy.insert.InsertCallback;

import java.io.File;
import java.io.IOException;

public class InsertBenchmark extends Benchmark{
	private IndexInterface idx;

	private final Long ticTime = 10000L;
	private final Long stmtTic = 10000L;

	private File outTime;

	private File outStmt;
	
	
	public InsertBenchmark(BenchmarkConfig config, String version, QTreeHashing hasher) {
		super(config, version, hasher);
		
		idx = null;
		if(config.getIndexFile(version,_hasher).exists() && !config.reRun()){
			log("loading index "+config.getIndexFile(version,_hasher));
			idx = IndexFactory.loadIndex(config.getIndexFile(version,_hasher));
		}else{
			idx = IndexFactory.createIndex(version, hasher, config);
		}
		outTime = new File(config.insertRoot(), config.getPrefix(version, _hasher)+"_insert."+ticTime+"ms.dat");
    	outStmt = new File(config.insertRoot(), config.getPrefix(version, _hasher)+"_insert."+stmtTic+"stmts.dat");
    	
    	log("output: "+outTime);
    	log("output: "+outStmt);
	}

	@Override
	public void benchmark() {
		InsertCallback cb = new InsertCallback(idx);
   	 	getConfig().insertRoot().mkdirs();
   	 	getConfig().plotRoot().mkdirs();
		Measure bench=null;
		Measure benchStmt=null;
		try {
			long start= System.currentTimeMillis(); 
			benchStmt = new Measure(cb,outStmt,stmtTic,true);
			bench = new Measure(benchStmt,outTime,ticTime,false);
			bench.startDocument();
			bench.indexLocal(getConfig().inputData());
			bench.endDocument();
			log("index.time "+_version+"-"+_hasher+" "+(System.currentTimeMillis()-start)+" "+ (double)benchStmt.getRealTime()/(double)benchStmt.getCounter() ,true);
			start= System.currentTimeMillis(); 
			idx.serialiseIndexToFile(getConfig().getIndexFile(_version, _hasher));
			log("serialised.out "+getConfig().getIndexFile(_version, _hasher)+" ("+getConfig().getIndexFile(_version, _hasher).exists()+")");
			log("serialised.time "+(System.currentTimeMillis()-start)+" (ms)");
			log("serialised.mem "+(getConfig().getIndexFile(_version, _hasher).length()/1024)+" (KBytes)",true);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void benchmarkPlot() {
		try {
			Printer.printInsertPlot(outTime, outStmt, getConfig().plotRoot());
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}

	@Override
	public boolean exists() {
		return outStmt.exists() && outTime.exists();
	}

}
