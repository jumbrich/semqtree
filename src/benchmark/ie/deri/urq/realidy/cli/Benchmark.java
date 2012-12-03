package ie.deri.urq.realidy.cli;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Benchmark extends CLIObject{
	private final static Logger log = LoggerFactory.getLogger(Benchmark.class);

	@Override
	protected void addOptions(Options opts) {
		//each benchmark needs an output file
		opts.addOption(CLIObject.OPTION_GENERIC_OUTPUT);
	}
	@Override
	protected void execute(CommandLine cmd) {
		File outDir = new File(cmd.getOptionValue(CLIObject.PARAM_GENERIC_OUTPUT));
		if(!outDir.exists()) outDir.mkdirs();
		
		
	}
		
	@Override
	public String getDescription() {
		return "benchmark SPARQL queries";
	}
}
