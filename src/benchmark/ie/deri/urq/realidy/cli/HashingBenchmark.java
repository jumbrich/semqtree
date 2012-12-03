package ie.deri.urq.realidy.cli;

import ie.deri.urq.realidy.hashing.HashDistCallback;
import ie.deri.urq.realidy.hashing.HashingFactory;
import ie.deri.urq.realidy.hashing.QTreeHashing;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HashingBenchmark extends CLIObject{
	private final static Logger log = LoggerFactory.getLogger(HashingBenchmark.class);

	@Override
	protected void addOptions(Options opts) {
		//each benchmark needs an output file
		opts.addOption(CLIObject.OPTION_GENERIC_OUTPUT);
		opts.addOption(CLIObject.OPTION_HASHER_NAME);
		opts.addOption(CLIObject.OPTION_MAX_DIMENSION);
		opts.addOption(CLIObject.OPTION_GENERIC_INPUT);
	}
	@Override
	protected void execute(CommandLine cmd) {
		File outDir = new File(cmd.getOptionValue(CLIObject.PARAM_GENERIC_OUTPUT));
		if(!outDir.exists()) outDir.mkdirs();
		String hasher = cmd.getOptionValue(CLIObject.PARAM_HASHER_NAME);
		Integer maxDim= Integer.valueOf(cmd.getOptionValue(CLIObject.PARAM_MAX_DIMENSION));
		File inputFile = new File(cmd.getOptionValue(CLIObject.PARAM_GENERIC_INPUT));
		
		QTreeHashing hashing = HashingFactory.createHasher(hasher, new int[]{0,0,0}, new int[]{maxDim,maxDim,maxDim});
		HashDistCallback cb;
		try {
			cb = new HashDistCallback(outDir,new File(outDir,"datahash-"+hasher+".points"), hashing, false);
			InputStream is = new  FileInputStream(inputFile);
			if(inputFile.getName().endsWith(".gz")){
				is = new GZIPInputStream(is);
			}
			cb.startDocument();
			NxParser nxp = new NxParser(is);
			while(nxp.hasNext()){
				cb.processStatement(nxp.next());
			}is.close();
			cb.endDocument();
			log.info("Result: {} {} {} {} ms", new Object[]{inputFile.getAbsolutePath(),hasher, maxDim,cb.getRealTime()});
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
		
	@Override
	public String getDescription() {
		return "benchmark hashing function";
	}
}
