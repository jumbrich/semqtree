package ie.deri.urq.realidy.cli;

import ie.deri.urq.realidy.index.SemQTree;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import de.ilmenau.datasum.index.QueryResultEstimation;

public class QREInfo extends CLIObject {

    @Override
    public String getDescription() {
    	return "print information about query result estimation object";
    }

	@Override
	protected void addOptions(Options opts) {
		opts.addOption(CLIObject.OPTION_GENERIC_INPUT);
		
	}

	@Override
	protected void execute(CommandLine cmd) {
		ObjectInputStream ois;
		try {
			ois = new ObjectInputStream(new FileInputStream(new File(cmd.getOptionValue(CLIObject.PARAM_GENERIC_INPUT))));
			QueryResultEstimation qre = (QueryResultEstimation) ois.readObject();
			ois.close();
			
			System.out.println(qre);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
