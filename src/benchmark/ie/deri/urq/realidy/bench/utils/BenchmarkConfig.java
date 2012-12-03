package ie.deri.urq.realidy.bench.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class BenchmarkConfig extends Properties{

	public static final String PROP_HASHING = "hashing";
	public static final String PROP_INSERT = "insert";
	public static final String PROP_HASHING_FUNCTION = "hashing_function";
	public static final String PROP_OUTPUT = "out_dir";
	
	public static final String PROP_MAX_DIM="max_dim";
	public static final String PROP_BUCKETS="buckets";
	public static final String PROP_FANOUT="fanout";

	public static final String PROP_INPUT_DATA = "input_data";
	public static final String PROP_STOREDETAILEDCOUNT = "storedetailedCount";
	public static final String PROP_INDEX_VERSION = "index_version";
	public static final String PROP_QUERY_FILE = "queries";
	public static final String PROP_QUERY = "query";
	public static final String PROP_RERUN = "rerun";


	
	
	public static BenchmarkConfig read(String optionValue) throws FileNotFoundException, IOException {
		BenchmarkConfig b = new BenchmarkConfig();
		b.load(new FileInputStream(optionValue));
		
		
		
		b.init();
		return b;
	}

	private File _out;
	private File _plot;
	private File _hashingFile;
	private File _inputData;
	private File _insert;
	private File _queries;
	private File _queryFile;

	public void init() {
		if(_out==null)
			_out = new File(getProperty(PROP_OUTPUT));
		if(_inputData == null)
			_inputData = new File(getProperty(PROP_INPUT_DATA));
		if(_queryFile == null)
			_queryFile = new File(getProperty(PROP_QUERY_FILE,null));
		
		if(!_out.exists())_out.mkdirs();
				
		_plot = new File(_out,"plot");
		if(!_plot.exists())_plot.mkdirs();
		
		_queries = new File(_out,"queries");
		_queries = new File(_queries, queryFile().getName());
		
		_hashingFile = new File(_out,"hashing");
		_insert = new File(_out,"insert");
	}

	public boolean hashing() {
		return new Boolean(this.getProperty(PROP_HASHING , "false"));
	}
	public boolean insert() {
		return new Boolean(this.getProperty(PROP_INSERT , "false"));
	}
	
	public File rootDir(){
		return _out;
	}
	

	public int maxDim() {
		return Integer.valueOf(getProperty(PROP_MAX_DIM,"-1"));
	}

	public File hashingRoot() {
		return _hashingFile;

	}

	public File inputData() {
		return _inputData;
	}

	public File plotRoot() {

		return _plot;
	}
	public File queryRoot() {
		return _queries;
	}	
	public File insertRoot() {
		return _insert;
	}
	
	public File queryFile() {
		return _queryFile;
	}

	public int maxBuckets() {
		return Integer.valueOf(getProperty(PROP_BUCKETS,"-1"));
	}

	public int maxFanout() {
		return Integer.valueOf(getProperty(PROP_FANOUT,"-1"));
	}

	public String[] hasher() {
		return this.getProperty(PROP_HASHING_FUNCTION,"").split(",");
	}

	public String[] indexVersion() {
		return getProperty(PROP_INDEX_VERSION,"").split(",");	}

	public boolean storeDetailedCount() {
		return new Boolean(this.getProperty(PROP_STOREDETAILEDCOUNT , "false"));
	}

	public File serFile() {
		return new File(_out,indexVersion()+"."+hasher()+".ser");
	}

	public boolean query() {
		return new Boolean(this.getProperty(PROP_QUERY , "false"));
	}

	public File jenaEvalRoot() {
		return new File(queryRoot(),"jenaEval");
	}
	public File jenaEvalQueryFile(String version, String hasher) {
		return jenaEvalQueryFile(version, hasher, queryFile());
	}
	public File jenaEvalQueryFile(String version, String hasher,File queryFile) {
		return new File(jenaEvalRoot(),"eval.query_"+version+"-"+hasher+"_"+queryFile.getName()+".qre.dat");
	}
	

	public boolean reRun() {
		return new Boolean(this.getProperty(PROP_RERUN , "true"));
	}

	public File getIndexFile(String version, String hasher) {
		return new File(indicesRoot(), getPrefix(version,hasher)+".ser");
	}

	private File indicesRoot() {
		return new File(rootDir(),"indices");
	}

	public String getPrefix(String version, String hasher) {
		return version+"-"+hasher;//+"-"+getID();
	}

	public void setHashingRoot(File outDir) {
		_hashingFile = outDir;
	}
	public void setPlotRoot(File dir) {
		_plot = dir;
	}

	public void setInputData(File localFile) {
		_inputData = localFile;
	}

	public File nxEvalRoot() {
		return new File(queryRoot(),"nxEval");
	}

	public File nxEvalQueryFile(String version, String hasher) {
		return new File(nxEvalRoot(),"eval.query_"+version+"-"+hasher+"_"+queryFile().getName()+".qre.dat");
	}

	public void setRootDir(File outRoot) {
		_out = outRoot;
	}

	public void setQueryFile(File query) {
		_queryFile =query;
		
	}

	public void setMaxDim(int i) {
		this.setProperty(PROP_MAX_DIM, ""+i);
		
	}
}
