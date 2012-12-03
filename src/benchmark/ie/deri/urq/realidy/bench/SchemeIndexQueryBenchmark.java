package ie.deri.urq.realidy.bench;

import ie.deri.urq.realidy.bench.utils.BenchmarkConfig;
import ie.deri.urq.realidy.bench.utils.MemoryMonitor;
import ie.deri.urq.realidy.bench.utils.QueryThread;
import ie.deri.urq.realidy.index.IndexInterface;
import ie.deri.urq.realidy.index.SchemaIndex;
import ie.deri.urq.realidy.insert.Indexer;
import ie.deri.urq.realidy.insert.InsertCallback;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;

import org.semanticweb.yars.nx.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.ilmenau.datasum.index.QueryResultEstimation;

public class SchemeIndexQueryBenchmark {
	private final static Logger log = LoggerFactory.getLogger(SchemeIndexQueryBenchmark.class);
	private static final long RUNTIME_THRESHOLD = 3*60; //seconds
	private static final Long MEMORY_THRESHOLD = 200*1024*1024L;
	private BenchmarkConfig config;
	private IndexInterface idx;
	
	public SchemeIndexQueryBenchmark(File query, File inputData,
			File outRoot) {
		config = new BenchmarkConfig();
		config.setQueryFile(query);
		config.setRootDir(outRoot);
		config.setInputData(inputData);
		config.init();
		
		idx = new SchemaIndex();
	}
	
	public void benchmark() {
		config.queryRoot().mkdirs();
		config.plotRoot().mkdirs();
		//we need to inser things
		long start = System.currentTimeMillis();
		InsertCallback cb = new InsertCallback(idx);
		try {
			Indexer.insertFromNXZ(config.inputData(), cb);
		} catch (ParseException e2) {
			e2.printStackTrace();
		} catch (IOException e2) {
			e2.printStackTrace();
		}
		
		log.info("starting query benchmark now");
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(config.queryFile()));
			MemoryMonitor mm = new MemoryMonitor(10000, null);
			mm.start();
			
			String query = null; 
			int counter =0;
		
			while((query=parseQuery(br))!=null){
				counter++;
				log.info("Q["+counter+"] Query\n--\n"+query+"\n--");
				
				QueryThread t = new QueryThread(idx,query);
				start = System.currentTimeMillis();
				t.start();
				while(t!=null && ! t.finished()){
					try {
						Thread.sleep(3000);
						boolean time = System.currentTimeMillis()-start > RUNTIME_THRESHOLD*1000;
						boolean mem = mm.getFreeBytes() < MEMORY_THRESHOLD;
						if( time || mem){
							log.info("Q["+counter+"] INTERRUPTED cause:  time:"+time+" sec  mem:"+mem +" ("+((mm.getFreeBytes()/(double)1024)/(double)1024)+" MB)");
							t.interrupt();
							t = null;
//							System.gc(d);System.gc();System.gc();System.gc();System.gc();
							Thread.sleep(1000);
							break;
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				log.info("Q["+counter+"] stopped result:"+(t!=null));
				
				//cleanup
//				
				
				QueryResultEstimation qre =null;
				if(t != null){
					qre=t.getQueryResultEstimation();
					log.info("get query result: "+(qre!=null));
				}
				if(qre!=null){
					serialiseQueryResultEstimation(qre, new File(config.queryRoot(),"query_"+idx.getVersionType()+".qre."+counter+".ser"));
					if(qre !=null && qre.getRelevantSourcesRanked()!=null)
						log.info("Q["+counter+"] "+qre.getRelevantSourcesRanked().size()+" src in "+(t.getElapsedTime())+" ms "+mm.monitorMemory());
				}
				else{log.info("Q["+counter+"] no results");}
				t = null;
				System.gc();System.gc();System.gc();System.gc();System.gc();
			}
		mm.stopMonitor();
		log.info("Q[ALL] stopped");
	} catch (FileNotFoundException e1) {
		e1.printStackTrace();
	} catch (IOException e) {
		e.printStackTrace();
	}	
	
	}

	
	
	public void benchmarkPlot() {
		
	}

	public boolean exists() {
		return false;
	}

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
