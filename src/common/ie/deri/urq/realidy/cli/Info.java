package ie.deri.urq.realidy.cli;

import ie.deri.urq.realidy.index.SemQTree;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

public class Info extends CLIObject {

    @Override
    public String getDescription() {
    	return "print information about index";
    }

	@Override
	protected void addOptions(Options opts) {
		opts.addOption(CLIObject.OPTION_REQUIRED_LOAD_INDEX);
		
	}

	@Override
	protected void execute(CommandLine cmd) {
		SemQTree sqt = SemQTree.loadIndex(new File(cmd.getOptionValue(PARAM_LOAD_INDEX)));
		System.out.println(sqt.info());
	}
}
