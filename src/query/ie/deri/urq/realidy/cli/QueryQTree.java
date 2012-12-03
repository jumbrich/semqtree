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

import de.ilmenau.datasum.exception.QTreeException;
import de.ilmenau.datasum.index.QueryResultEstimation;

public class QueryQTree extends CLIObject {

	@Override
	protected void addOptions(Options opts) {
		opts.addOption(CLIObject.OPTION_REQUIRED_LOAD_INDEX);
		opts.addOption(CLIObject.OPTION_QUERY_FILE);
		opts.addOption(CLIObject.OPTION_DEBUG);
		}

	@Override
	protected void execute(CommandLine cmd) {
		SemQTree sqt = loadIndex(new File(cmd.getOptionValue(CLIObject.PARAM_LOAD_INDEX)));
		try {
			String queryString = readQuery(new File(cmd.getOptionValue(CLIObject.PARAM_QUERY_FILE)));
			sqt.enableDebugMode(cmd.hasOption(CLIObject.PARAM_DEBUG));

			QueryResultEstimation qre = sqt.evaluateQuery(queryString);
			System.out.println(qre);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void formatOutput(ResultSet set, CommandLine cmd) {
		ResultSetFormatter.outputAsRDF(System.out, ResultsFormat.FMT_RDF_TTL.getSymbol(), set);
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
