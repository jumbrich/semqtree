package ie.deri.urq.realidy.bench;

import ie.deri.urq.realidy.bench.utils.BenchmarkConfig;
import ie.deri.urq.realidy.bench.utils.MemoryMonitor;
import ie.deri.urq.realidy.bench.utils.QueryThread;
import ie.deri.urq.realidy.hashing.QTreeHashing;
import ie.deri.urq.realidy.index.SemQTree;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;

import de.ilmenau.datasum.index.QueryResultEstimation;

public class IndexQueryNonOptimisedBenchmark extends Benchmark {

	private static final long RUNTIME_THRESHOLD = 3*60; //seconds
	private static final Long MEMORY_THRESHOLD = 200*1024*1024L;
	private SemQTree sqt;

	public IndexQueryNonOptimisedBenchmark(BenchmarkConfig config, String version,
			QTreeHashing hashing) {
		super(config,version ,hashing);
		sqt = SemQTree.loadIndex(config.getIndexFile(version,hashing.getHasherName()));
	}

	public IndexQueryNonOptimisedBenchmark(SemQTree sqt, File query, File inputData,
			File outRoot) {
		super(new BenchmarkConfig(), sqt.getVersionType(),sqt.getHasher());
		this.sqt = sqt;
		getConfig().setQueryFile(query);
		getConfig().setRootDir(outRoot);
		getConfig().setInputData(inputData);
		getConfig().init();

	}

	@Override
	public void benchmark() {
		getConfig().queryRoot().mkdirs();
		getConfig().plotRoot().mkdirs();
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(getConfig().queryFile()));
			MemoryMonitor mm = new MemoryMonitor(10000, null);
			mm.start();
			
			String query = null; 
			int counter =0;

			while((query=parseQuery(br))!=null){
				counter++;
				log("Q["+counter+"] Query\n--\n"+query+"\n--");

				QueryThread t = new QueryThread(sqt,query,false);
				long start = System.currentTimeMillis();
				t.start();
				while(!t.finished()){
					try {
						Thread.sleep(3000);
						boolean time = System.currentTimeMillis()-start > RUNTIME_THRESHOLD*1000;
						boolean mem = mm.getFreeBytes() < MEMORY_THRESHOLD;
						if( time || mem){
							log("Q["+counter+"] INTERRUPTED cause: time:"+time+" mem:"+mem +"("+mm.getFreeBytes()+")");
							t.interrupt();
							break;
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				QueryResultEstimation qre=t.getQueryResultEstimation();
				if(qre!=null){
					serialiseQueryResultEstimation(qre, new File(getConfig().queryRoot(),"query-no-opt_"+sqt.getVersionType()+"-"+sqt.getAbstractIndex().getHasher()+".qre."+counter+".ser"));
					if(qre !=null && qre.getRelevantSourcesRanked()!=null)
						log("Q["+counter+"] "+qre.getRelevantSourcesRanked().size()+" src in "+(t.getElapsedTime())+" ms "+mm.monitorMemory());
				}
				t = null;
			}
			mm.stopMonitor();
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}	
	}



	@Override
	public void benchmarkPlot() {

	}

	@Override
	public boolean exists() {
		return false;
	}

	@Override
	public String summary() {
		return null;
	}

	private void serialiseQueryResultEstimation(QueryResultEstimation qre,
			File file) throws IOException {
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file));
		oos.writeObject(qre);
		oos.close();
	}

	private String parseQuery(BufferedReader br) throws IOException {
		String line = null;
		StringBuilder sb = new StringBuilder();
		while((line = br.readLine()) != null){
			if(line.trim().length()==0) break;
			sb.append(line).append("\n");
		}
		if(line==null && sb.length()==0) return null;
		return sb.toString();
	}


}
