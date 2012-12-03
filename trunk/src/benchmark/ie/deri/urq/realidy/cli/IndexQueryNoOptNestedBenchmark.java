package ie.deri.urq.realidy.cli;

import ie.deri.urq.realidy.bench.utils.MemoryMonitor;
import ie.deri.urq.realidy.bench.utils.QueryThread;
import ie.deri.urq.realidy.index.IndexFactory;
import ie.deri.urq.realidy.index.IndexInterface;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.ilmenau.datasum.index.QueryResultEstimation;

public class IndexQueryNoOptNestedBenchmark extends CLIObject{
	private final static Logger log = LoggerFactory.getLogger(IndexQueryNoOptNestedBenchmark.class);

	private static final long RUNTIME_THRESHOLD = 3*60; //seconds
	private static final Long MEMORY_THRESHOLD = 200*1024*1024L;

	@Override
	protected void addOptions(Options opts) {
		//each benchmark needs an output file
		opts.addOption(CLIObject.OPTION_GENERIC_OUTPUT);
		opts.addOption(CLIObject.OPTION_REQUIRED_LOAD_INDEX);
		opts.addOption(CLIObject.OPTION_QUERY_FILE);
	}
	@Override
	protected void execute(CommandLine cmd) {
		File outDir = new File(cmd.getOptionValue(CLIObject.PARAM_GENERIC_OUTPUT));
		if(!outDir.exists()) outDir.mkdirs();
		File index = new File(cmd.getOptionValue(CLIObject.PARAM_LOAD_INDEX));
		File query = new File(cmd.getOptionValue(CLIObject.PARAM_QUERY_FILE));
		IndexInterface idx = IndexFactory.loadIndex(index);
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(query));
			MemoryMonitor mm = new MemoryMonitor(10000, null);
			mm.start();

			String sparql = parseQuery(br); 

			int queryNo = Integer.parseInt(query.getName().substring(query.getName().indexOf(".")+1,query.getName().lastIndexOf(".")));
			if(sparql != null)
				log.info("Q["+queryNo+"] Query\n--\n"+query+"\n--");

			QueryThread t = new QueryThread(idx,sparql,false);
			long start = System.currentTimeMillis();
			t.start();
			while(!t.finished()){
				try {
					Thread.sleep(3000);
					boolean time = System.currentTimeMillis()-start > RUNTIME_THRESHOLD*1000;
					boolean mem = mm.getFreeBytes() < MEMORY_THRESHOLD;
					if( time || mem){
						log.info("Q["+queryNo+"] INTERRUPTED cause: time:"+time+" mem:"+mem +"("+mm.getFreeBytes()+")");
						t.interrupt();
						break;
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			QueryResultEstimation qre=t.getQueryResultEstimation();
			if(qre!=null){
				serialiseQueryResultEstimation(qre, new File(outDir,idx.getLabel()+".no_opt_nested."+query.getName()+".ser"));
				if(qre !=null && qre.getRelevantSourcesRanked()!=null)
					log.info("Q["+queryNo+"] "+qre.getRelevantSourcesRanked().size()+" src in "+(t.getElapsedTime())+" ms "+mm.monitorMemory());
			}
			t = null;
			mm.stopMonitor();
	} catch (FileNotFoundException e1) {
		e1.printStackTrace();
	} catch (IOException e) {
		e.printStackTrace();
	}		
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

@Override
public String getDescription() {
	return "benchmark hashing function";
}
}
