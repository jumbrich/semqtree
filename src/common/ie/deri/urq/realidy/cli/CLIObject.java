package ie.deri.urq.realidy.cli;

import java.util.Arrays;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CLIObject {
	private final static Logger log = LoggerFactory.getLogger(CLIObject.class);
	private Options _opts;

	public abstract String getDescription();

	public String getCommand(){
		return this.getClass().getSimpleName();
	}

	/*
	 * Commandline parameters
	 */
	
	private static final String PARAM_HELP = "?";
	private static final String PARAM_HELP1 = "h";
	
	public static final String PARAM_INDEX_VERSION = "version";
	public static final String PARAM_LOAD_INDEX = "index";
	public static final String PARAM_OUTPUT_DIR = "od";
	public static final String PARAM_BUCKET_NUMBER = "b";
	public static final String PARAM_FANOUT_NUMBER = "f";;
	public static final String PARAM_HASHER_NAME = "hash";
	public static final String PARAM_MAX_DIMENSION = "m";
	public static final String PARAM_STORE_DETAILED_COUNT = "d"; 
	public static final String PARAM_LOCAL_INSERT = "local";
	public static final String PARAM_URI_INSERT = "uri";
	public static final String PARAM_BENCH_MARK_DIR = "bd";
	public static final String PARAM_THREAD_NUMBER = "t";
	public static final String PARAM_MAX_CRAWLING_ROUNDS = "r";
	public static final String PARAM_LIST_HASHER = "lh";
	public static final String PARAM_QUERY_FILE ="q";
	public static final String PARAM_DEBUG = "D";
	public static final String PARAM_SAMPLING_SIZE= "size";
	public static final String PARAM_GENERIC_INPUT = "i";
	public static final String PARAM_GENERIC_OUTPUT = "o";
	public static final String PARAM_NUMBER_OF_JOINS = "joins";
	public static final String PARAM_DEBUG_LONG = "debug";

	public static final String PARAM_SKIP_HASH = "shash";
	public static final String PARAM_SKIP_INSERT = "sinsert";
	public static final String PARAM_SKIP_NONOPT = "snoopt";
	public static final String PARAM_SKIP_INVIDX = "sinvidx";
	public static final String PARAM_SKIP_NESTED = "snested";
	public static final String PARAM_SKIP_NX = "snxeval";
	public static final String PARAM_MAX_BUCKETS = "buckets";
	public static final String PARAM_MAX_FANOUT = "fanout";
	
	public static final Integer DEFAULT_BUCKET_NUMBER = 10000;
	public static final Integer DEFAULT_FANOUT_NUMBER = 20;
	public static final Integer DEFAULT_THREAD_NUMBER = 2;
	public static final Integer DEFAULT_MAX_DIMENSION = 50000;
	public static final Integer DEFAULT_MAX_CRAWLING_ROUNDS = 0;
	public static final String DEFAULT_HASHER_NAME = "prefix";
	
	
	
	/*
	 * Commandline Options
	 */
	/**
	 * LOAD QTREE
	 */
	public static final Option OPTION_REQUIRED_LOAD_INDEX = OptionBuilder.withArgName("index").isRequired(true)
	.hasArgs(1)
	.withDescription("location of the index")
	.create(PARAM_LOAD_INDEX);

	/**
	 * HELP
	 */
	private static final OptionGroup OPTIONGROUP_HELP = new OptionGroup(); 
	static{
		OPTIONGROUP_HELP.addOption(OptionBuilder.withArgName("help")
				.hasArgs(0)
				.withDescription("print help screen")
				.create(PARAM_HELP));
		OPTIONGROUP_HELP.addOption(OptionBuilder.withArgName("help")
		.hasArgs(0)
		.withDescription("print help screen")
		.create(PARAM_HELP1));
	}
	
	
	/**
	 * GENERAL OUTPUT DIR
	 */
	public static final Option OPTION_REQUIRED_OUTPUT_DIR =  OptionBuilder.isRequired(true).withArgName("out")
	.hasArgs(1)
	.withDescription("output directory")
	.create(PARAM_OUTPUT_DIR);
	
	
	/**
	 * BUCKET NUMBER
	 */
	public static final Option OPTION_BUCKET_NUMBER = OptionBuilder.withArgName("buckets")
	.hasArgs(1)
	.withDescription("number of buckets (default:"+DEFAULT_BUCKET_NUMBER+")")
	.create(PARAM_BUCKET_NUMBER);
	
	/**
	 * FANOUT VALUE
	 */
	public static final Option OPTION_FANOUT_NUMBER = OptionBuilder.withArgName("fanout")
	.hasArgs(1)
	.withDescription("fanout value (default:"+DEFAULT_FANOUT_NUMBER+")")
	.create(PARAM_FANOUT_NUMBER);
	
	
	public static final Option OPTION_HASHER_NAME = OptionBuilder.withArgName("name")
	.hasArgs(Option.UNLIMITED_VALUES)
	.withDescription("hasher name (default: "+DEFAULT_HASHER_NAME+")")
	.create(PARAM_HASHER_NAME);
	
	public static final Option OPTION_LIST_HASHER = OptionBuilder.withArgName("flag")
	.hasArgs(0)
	.withDescription("list available hash functions")
	.create(PARAM_LIST_HASHER);
	
	
	
	public static final Option OPTION_MAX_DIMENSION = OptionBuilder.withArgName("maximum")
	.hasArgs(1).withLongOpt(PARAM_DEBUG_LONG)
	.withDescription("max dimension for qtree (default:"+DEFAULT_MAX_DIMENSION)
	.create(PARAM_MAX_DIMENSION);
	
	public static final Option OPTION_STORE_DETAILED_COUNT = OptionBuilder.withArgName("flag")
	.hasArgs(0)
	.withDescription("index with detailed counts")
	.create(PARAM_STORE_DETAILED_COUNT);
	
	public static final Option OPTION_LOCAL_INSERT = OptionBuilder.withArgName("file")
	.hasArgs(1)
	.withDescription("location on disk")
	.create(PARAM_LOCAL_INSERT);
	
	public static final Option OPTION_URI_INSERT = OptionBuilder.withArgName("uri")
	.hasArgs(1)
	.withDescription("insert content from an uri")
	.create(PARAM_URI_INSERT);
	
	public static final OptionGroup OPTIONGROUP_INSERT_MODI = new OptionGroup();
	static{
		OPTIONGROUP_INSERT_MODI.addOption(OPTION_LOCAL_INSERT);
		OPTIONGROUP_INSERT_MODI.addOption(OPTION_URI_INSERT);
	}
	
	public static final Option OPTION_OUTPUT_DIR = OptionBuilder.withArgName("out")
	.hasArgs(1)
	.withDescription("output directory")
	.create(PARAM_OUTPUT_DIR);
	
	public static final Option OPTION_BENCH_MARK_DIR = OptionBuilder.withArgName("bench dir")
	.hasArgs(1)
	.withDescription("location to store benchmarks")
	.create(PARAM_BENCH_MARK_DIR);
	
	public static final Option OPTION_THREAD_NUMBER = OptionBuilder.withArgName("buckets")
	.hasArgs(1)
	.withDescription("number of threads (default:"+DEFAULT_THREAD_NUMBER)
	.create(PARAM_THREAD_NUMBER);
	
	public static final Option OPTION_MAX_CRAWLING_ROUNDS = OptionBuilder.withArgName("rounds")
	.hasArgs(1)
	.withDescription("number of crawling rounds (default:"+DEFAULT_MAX_CRAWLING_ROUNDS)
	.create(PARAM_MAX_CRAWLING_ROUNDS);

	public static final Option OPTION_QUERY_FILE = OptionBuilder.withArgName("query")
	.hasArgs(1)
	.withDescription("SPARQL query")
	.create(PARAM_QUERY_FILE);
	
	public static final Option OPTION_DEBUG = OptionBuilder.withArgName("flag")
	.hasArgs(0)
	.withDescription("debug mode")
	.create(PARAM_DEBUG);
	
	public static final Option OPTION_NUMBER_OF_JOINS  = OptionBuilder.withArgName("joins")
	.hasArgs(1)
	.withDescription("number of joins")
	.create(PARAM_NUMBER_OF_JOINS);
	public static final Option OPTION_SAMPLING_SIZE = OptionBuilder.withArgName("size")
	.hasArgs(1)
	.withDescription("number of samples")
	.create(PARAM_SAMPLING_SIZE);
	
	public static final Option OPTION_GENERIC_INPUT = OptionBuilder.withArgName("input")
	.hasArgs(1)
	.withDescription("input file")
	.create(PARAM_GENERIC_INPUT);
	public static final Option OPTION_GENERIC_OUTPUT = OptionBuilder.withArgName("output")
	.hasArgs(1)
	.withDescription("output file")
	.create(PARAM_GENERIC_OUTPUT);
	public static final String PARAM__WITH_DL_APPROACH = "dl";
	public static final Option OPTION_WITH_DL_APPROACH = OptionBuilder.withArgName("flag")
	.hasArgs(0)
	.withDescription("enable dl approach")
	.create(PARAM__WITH_DL_APPROACH);
	public static final String PARAM_TOPK = "topk";
	public static final Option OPTION_TOPK = OptionBuilder.withArgName("topK")
	.hasArgs(1)
	.withDescription("use topK sources")
	.create(PARAM_TOPK);
	public static final String PARAM_OUTPUT_FORMAT = "format";
	public static final Option OPTION_OUTPUT_FORMAT = OptionBuilder.withArgName("format")
	.hasArgs(1)
	.withDescription("RDF/XML , N3, Turtle, N-TRIPLES")
	.create(PARAM_OUTPUT_FORMAT);
	
	public static final String PARAM_BENCH_CONFIG_FILE = "config";
	public static final Option OPTION_BENCH_CONFIG_FILE = OptionBuilder.withArgName("file")
	.hasArgs(1)
	.withDescription("benchmark config file.")
	.create(PARAM_BENCH_CONFIG_FILE);
	
	public static final Option OPTION_SKIP_HASH = OptionBuilder.withArgName("flag")
	.hasArgs(0)
	.withDescription("skip hashing benchmark.")
	.create(PARAM_SKIP_HASH);
	
	public static final Option OPTION_SKIP_INSERT = OptionBuilder.withArgName("flag")
	.hasArgs(0)
	.withDescription("skip insert benchmark.")
	.create(PARAM_SKIP_INSERT);
	
	public static final Option OPTION_SKIP_NONOPT  = OptionBuilder.withArgName("flag")
	.hasArgs(0)
	.withDescription("skip no opt benchmark.")
	.create(PARAM_SKIP_NONOPT);
	public static final Option OPTION_SKIP_INVIDX  = OptionBuilder.withArgName("flag")
	.hasArgs(0)
	.withDescription("skip inv idx benchmark.")
	.create(PARAM_SKIP_INVIDX);
	public static final Option OPTION_SKIP_NESTED  = OptionBuilder.withArgName("flag")
	.hasArgs(0)
	.withDescription("skip nested benchmark.")
	.create(PARAM_SKIP_NESTED);
	public static final Option OPTION_SKIP_NX  = OptionBuilder.withArgName("flag")
	.hasArgs(0)
	.withDescription("skip nx eval benchmark.")
	.create(PARAM_SKIP_NX);
	
	public static final Option OPTION_INDEX_VERSION  = OptionBuilder.withArgName("index version")
	.hasArgs(1)
	.withDescription("qtree, histo, schema, inv-uri.")
	.create(PARAM_INDEX_VERSION);
	
	public static final Option OPTION_MAX_BUCKEST  = OptionBuilder.withArgName("buckets")
	.hasArgs(1)
	.withDescription("maximum number of buckets")
	.create(PARAM_MAX_BUCKETS);
	
	public static final Option OPTION_MAX_FANOUT  = OptionBuilder.withArgName("fanout")
	.hasArgs(1)
	.withDescription("fanout of qtree ( normally around 20).")
	.create(PARAM_MAX_FANOUT);
	
	private void init() {
		_opts = new Options();
		_opts.addOptionGroup(OPTIONGROUP_HELP);
		addOptions(_opts);
	}

	/**
	 * add all Option(Groups) to this object
	 * Note: The help flag is set automatically ("?")
	 * @param opts
	 */
	abstract protected void addOptions(Options opts);

	public void run(String[] args) {
		log.info("=======[START] ({})",Arrays.toString(args));
		
		CommandLine cmd = verifyArgs(args);
		
		long start = System.currentTimeMillis();
		execute(cmd);
		
		long end = System.currentTimeMillis();
		log.info("=======[END] ({} ms)",(end-start));
		System.exit(0);
		log.info("System.exit");
	}

	abstract protected void execute(CommandLine cmd);

	protected CommandLine verifyArgs(String[] args) {
		init();

		CommandLineParser parser = new BasicParser();
		CommandLine cmd = null;

		try {
			cmd = parser.parse(_opts, args);
			
		} catch (org.apache.commons.cli.ParseException e) {
			log.info("ERROR: {} : {}",new Object[]{e.getClass().getSimpleName(),e.getMessage()});
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(this.getClass().getSimpleName(), _opts ,true);
			System.exit(-1);
		}
		if(  cmd.hasOption(CLIObject.PARAM_HELP)||cmd.hasOption(CLIObject.PARAM_HELP1)){
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(this.getClass().getSimpleName(), _opts ,true);
			System.exit(-1);
		}

		return cmd;
	}


}
