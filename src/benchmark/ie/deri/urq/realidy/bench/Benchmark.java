	package ie.deri.urq.realidy.bench;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ie.deri.urq.realidy.bench.utils.BenchmarkConfig;
import ie.deri.urq.realidy.bench.utils.Formater;
import ie.deri.urq.realidy.hashing.QTreeHashing;

public abstract class Benchmark {
	private final static Logger log = LoggerFactory.getLogger(Benchmark.class);
	
	private final BenchmarkConfig _config;
	protected String _version;
	protected String _hasher ="no_hash";
	private long _elapsedTime;
	private final StringBuilder logBuffer; 

	public Benchmark(BenchmarkConfig config, String version,	QTreeHashing hasher){
		_config = config;
		_version = version;
		if(hasher != null)
			_hasher = hasher.getHasherName();
		logBuffer = new StringBuilder();
	}
	
	
    public void benchmarkDisk(){
    	long start = System.currentTimeMillis();
    	benchmark();
    	_elapsedTime = System.currentTimeMillis()-start;
    }
    
    abstract public void benchmark();
    abstract public void benchmarkPlot();

    public void log(String msg){
    	log(msg,false);
    }
    public void log(String msg, boolean b) {
    	log.info(msg);
    	if(b)
    		logBuffer.append(msg).append("\n");
		
	}
    
    
	public String summary(){
		StringBuilder sb = new StringBuilder("Summary [");
		sb.append(_config.getPrefix(_version, _hasher)).append("]\n");
		sb.append(logBuffer.toString());
		sb.append("\n ---------------------");
		sb.append("\n time: "+Formater.readableTime(_elapsedTime));
		sb.append("\n ---------------------\n");
		
		return sb.toString();
	}

	abstract public boolean exists();
	
	
	public BenchmarkConfig getConfig() {
		return _config;
	}


	public boolean valid() {
			return (_config.reRun() || !exists());
	}
}
