package ie.deri.urq.realidy.cli;

import ie.deri.urq.realidy.index.SemQTree;
import ie.deri.urq.realidy.insert.Indexer;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

import de.ilmenau.datasum.index.AbstractIndex;

public class Insert extends CLIObject {

    private static long s_Load;
    private static long e_Load;
    private static long s_Insert;
    private static long e_Insert;
    private static long s_Serialise;
    private static long e_Serialise;

    public String getDescription() {
	return "Insert stmts in exisiting index";
    }

    public static void main(String[] args) {
	long start = System.currentTimeMillis();
	Options options = new Options();

	Option qtree = new Option("qt", "on-disk location of serialised index");
	qtree.setArgs(1);
	qtree.setRequired(true);
	options.addOption(qtree);
	
	OptionGroup group = new OptionGroup();
	group.setRequired(true);
	group.addOption(new Option("uri",true, "insert content from a URI"));
	options.addOptionGroup(group);
	
	CommandLineParser parser = new BasicParser();
	CommandLine cmd = null;
	try {
	    cmd = parser.parse(options, args);
	} catch (org.apache.commons.cli.ParseException e) {
	    System.err.println("ERROR: " + e.getClass().getSimpleName() + ": " + e.getMessage());
	    HelpFormatter formatter = new HelpFormatter();
	    formatter.printHelp(Insert.class.getSimpleName(), options ,true);
	    return;
	}
	
	
	
    }

    private static void save(SemQTree idx, File indexFile) {
	s_Serialise = System.currentTimeMillis();
	idx.serialiseIndexToFile(indexFile);
	e_Serialise = System.currentTimeMillis();
	
    }

    private static void insert(SemQTree idx, CommandLine cmd) {
    	s_Insert = System.currentTimeMillis();
    	if(idx != null && cmd.hasOption(CLIObject.PARAM_URI_INSERT)){
    	    try {
    	    	URI uri = new URI(cmd.getOptionValue(CLIObject.PARAM_URI_INSERT));
    	    	
    	    	int threads = (cmd.hasOption(CLIObject.PARAM_THREAD_NUMBER)) ? Integer.parseInt(cmd.getOptionValue(CLIObject.PARAM_THREAD_NUMBER)) : CLIObject.DEFAULT_THREAD_NUMBER;
    			int hops = (cmd.hasOption(CLIObject.PARAM_MAX_CRAWLING_ROUNDS)) ?  Integer.parseInt(cmd.getOptionValue(CLIObject.PARAM_MAX_CRAWLING_ROUNDS)) :  CLIObject.DEFAULT_MAX_CRAWLING_ROUNDS;
    	    	
    	    	Indexer.insertFromWeb(uri, idx, threads, hops);
    	    } catch (URISyntaxException e) {
    	    	System.err.println("Input uri "+cmd.getOptionValue(CLIObject.PARAM_URI_INSERT)+" is not a valid uri: "+e.getMessage());
    	    }
    	}
    	else if(idx != null && cmd.hasOption(CLIObject.PARAM_LOCAL_INSERT)){
    		try {
    			Indexer.insertFromNXZ(new File(cmd.getOptionValue(CLIObject.PARAM_LOCAL_INSERT)), idx);
    		} catch (Exception e) {
    			System.err.println("Input file "+cmd.getOptionValue(CLIObject.PARAM_LOCAL_INSERT)+" caused: "+e.getMessage());
    		}
    	}
    	e_Insert = System.currentTimeMillis();

        }
    
    private static SemQTree loadIndex(File indexFile) {
    	s_Load = System.currentTimeMillis();
    	SemQTree index = SemQTree.loadIndex(indexFile);
    	e_Load = System.currentTimeMillis();
    	return index;
	
    }

	@Override
	protected void addOptions(Options opts) {
		opts.addOption(OPTION_REQUIRED_LOAD_INDEX);
		opts.addOptionGroup(OPTIONGROUP_INSERT_MODI);
	}

	@Override
	protected void execute(CommandLine cmd) {
		long start = System.currentTimeMillis();
		//load index
		File indexFile = new File(cmd.getOptionValue(PARAM_LOAD_INDEX));
		if(indexFile.exists()) {
		    SemQTree idx = loadIndex(indexFile);
		    if(idx!=null){
			insert(idx,cmd);
			
			save(idx,indexFile);
			System.err.println(idx.info());
		    }
		    else{
		    }
		}else{
		    System.err.println("ERROR: Index location ("+indexFile.getAbsolutePath()+") does not exist");
		}
		System.err.println("----------DONE------------");
		System.err.println("total: "+(System.currentTimeMillis()-start)+" ms");
		System.err.println(" load: "+(e_Load-s_Load)+" ms");
		System.err.println(" insert: "+(e_Insert-s_Insert)+" ms");
		System.err.println(" save: "+(e_Serialise-s_Serialise)+" ms");
		
	}
}
