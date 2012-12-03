package ie.deri.urq.realidy.cli;

import ie.deri.urq.realidy.bench.utils.MemoryMonitor;
import ie.deri.urq.realidy.index.SemQTree;

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

import de.ilmenau.datasum.exception.QTreeException;
import de.ilmenau.datasum.index.QueryResultEstimation;

public class BenchmarkQueries extends CLIObject{
	private final static Logger log = LoggerFactory.getLogger(BenchmarkQueries.class);

	@Override
	protected void addOptions(Options opts) {
		opts.addOption(CLIObject.OPTION_QUERY_FILE);
		opts.addOption(CLIObject.OPTION_OUTPUT_DIR);
		opts.addOption(CLIObject.OPTION_REQUIRED_LOAD_INDEX);
	}
	@Override
	protected void execute(CommandLine cmd) {
		SemQTree sqt = SemQTree.loadIndex(new File(cmd.getOptionValue(CLIObject.PARAM_LOAD_INDEX)));
		File sparqlQueries = new File(cmd.getOptionValue(CLIObject.PARAM_QUERY_FILE));
		File output = new File(cmd.getOptionValue(CLIObject.PARAM_OUTPUT_DIR));
		if(!output.exists())output.mkdirs();
		benchmark(sqt, sparqlQueries, output);
	}
		public void benchmark(SemQTree sqt, File sparqlQueries, File output){
		output.mkdirs();
			BufferedReader br;
		
			
		try {
			br = new BufferedReader(new FileReader(sparqlQueries));
			MemoryMonitor mm = new MemoryMonitor(0, null);
		
			String query = null; 
			int counter =0;
			
			while((query=parseQuery(br))!=null){
				
				counter++;
				log.info("Benchmarking query "+counter+"\n"+query);
				QueryResultEstimation qre=null;
				long start = System.currentTimeMillis();
				try {
					qre = sqt.evaluateQuery(query);
					qre.setQueryString(query);
					serialiseQueryResultEstimation(qre, new File(output,"query_"+sqt.getVersionType()+"-"+sqt.getAbstractIndex().getHasher()+"_"+sparqlQueries.getName()+".qre."+counter+".ser"));
				} catch (QTreeException e) {
					e.printStackTrace();
				}finally{
					if(qre !=null && qre.getRelevantSourcesRanked()!=null)
						log.info(">>[Q-"+counter+"] "+qre.getRelevantSourcesRanked().size()+" src in "+(System.currentTimeMillis()-start)+" ms "+mm.monitorMemory());
				}
			}
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
		return "benchmark SPARQL queries";
	}
}
