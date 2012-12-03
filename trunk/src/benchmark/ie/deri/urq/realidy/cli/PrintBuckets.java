package ie.deri.urq.realidy.cli;

import ie.deri.urq.realidy.index.SemQTree;

import java.io.File;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

public class PrintBuckets extends CLIObject{

	@Override
	public String getDescription() {
		return "Benchmark of the insert operation";
	}

	@Override
	protected void addOptions(Options opts) {
		opts.addOption(CLIObject.OPTION_REQUIRED_LOAD_INDEX);
		opts.addOption(CLIObject.OPTION_REQUIRED_OUTPUT_DIR);
	}

	@Override
	protected void execute(CommandLine cmd) {
		
		SemQTree idx = SemQTree.loadIndex(new File(cmd.getOptionValue(CLIObject.PARAM_LOAD_INDEX)));
		File output = new File(cmd.getOptionValue(PARAM_OUTPUT_DIR));
		try {
			ie.deri.urq.realidy.bench.utils.Printer.printBuckets(idx, output,output);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		PrintBuckets p = new PrintBuckets();
		p.run(args);
	}
	
}
