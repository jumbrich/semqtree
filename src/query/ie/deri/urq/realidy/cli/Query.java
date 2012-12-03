package ie.deri.urq.realidy.cli;

import ie.deri.urq.realidy.index.SemQTree;
import ie.deri.urq.realidy.query.arq.QueryExecutor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.sparql.resultset.ResultsFormat;

public class Query extends CLIObject {

	@Override
	protected void addOptions(Options opts) {
		opts.addOption(CLIObject.OPTION_REQUIRED_LOAD_INDEX);
		opts.addOption(CLIObject.OPTION_QUERY_FILE);
		opts.addOption(CLIObject.OPTION_THREAD_NUMBER);
		opts.addOption(CLIObject.OPTION_WITH_DL_APPROACH);
		opts.addOption(CLIObject.OPTION_TOPK);
		opts.addOption(CLIObject.OPTION_OUTPUT_FORMAT);
	}

	@Override
	protected void execute(CommandLine cmd) {
		SemQTree sqt = loadIndex(new File(cmd.getOptionValue(CLIObject.PARAM_LOAD_INDEX)));
		try {
			String queryString = readQuery(new File(cmd.getOptionValue(CLIObject.PARAM_QUERY_FILE)));
			QueryExecutor executor = new QueryExecutor(sqt, cmd.hasOption(CLIObject.PARAM__WITH_DL_APPROACH));
			sqt.enableDebugMode(cmd.hasOption(CLIObject.PARAM_DEBUG));
			int thread = (cmd.hasOption(PARAM_THREAD_NUMBER)) ? Integer.valueOf(cmd.getOptionValue(PARAM_THREAD_NUMBER)): 2;
			int topK = (cmd.hasOption(PARAM_TOPK)) ? Integer.valueOf(cmd.getOptionValue(PARAM_TOPK)): 200;
			ResultSet set = executor.executeQuery(queryString,thread,topK,null);
			
			formatOutput(set,cmd);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void formatOutput(ResultSet set, CommandLine cmd) {
		String format = ResultsFormat.FMT_RDF_TTL.getSymbol();
		if(cmd.hasOption(PARAM_OUTPUT_FORMAT)){
			format = cmd.getOptionValue(PARAM_OUTPUT_FORMAT);
		}
		ResultSetFormatter.outputAsRDF(System.out, format, set);
	}

	private String readQuery(File queryFile) throws IOException {
		// 
		StringBuilder sb = new StringBuilder();
		BufferedReader br = new BufferedReader(new FileReader(queryFile));
		String line = null;
		while((line = br.readLine())!=null)
			sb.append(line+"\n");
		return sb.toString();
	}

	@Override
	public String getDescription() {
		return "executes a query with the specific index";
	}

	
	  private static SemQTree loadIndex(File indexFile) {
		  SemQTree index = SemQTree.loadIndex(indexFile);
		  return index;
	    }
}
