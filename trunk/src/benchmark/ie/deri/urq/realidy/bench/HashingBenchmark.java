package ie.deri.urq.realidy.bench;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;

import ie.deri.urq.realidy.bench.utils.BenchmarkConfig;
import ie.deri.urq.realidy.bench.utils.Printer;
import ie.deri.urq.realidy.hashing.HashDistCallback;
import ie.deri.urq.realidy.hashing.QTreeHashing;

public class HashingBenchmark extends Benchmark {

	private File _datFile;
	private QTreeHashing _hashing;
	private long _totalTime;
	public HashingBenchmark(BenchmarkConfig config, String version,
			QTreeHashing hashing) {
		super(config,version ,hashing);
		_datFile = new File(config.hashingRoot(),"datahash-"+hashing.getHasherName()+".points");
		_hashing = hashing;
		
	}

	
	@Override
	public void benchmark() {
		HashDistCallback cb;
		try {
			cb = new HashDistCallback(getConfig().hashingRoot(),_datFile, _hashing, false);
			InputStream is = new  FileInputStream(getConfig().inputData());
			if(getConfig().inputData().getName().endsWith(".gz")){
				is = new GZIPInputStream(is);
			}
			cb.startDocument();
			NxParser nxp = new NxParser(is);
			while(nxp.hasNext()){
				cb.processStatement(nxp.next());
			}is.close();
			cb.endDocument();
			_totalTime = cb.getRealTime();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void benchmarkPlot() {
		try {
			Printer.printHashDistPlot(1,getConfig().hashingRoot(),_hasher,getConfig().plotRoot());
			Printer.printHashDistPlot(2,getConfig().hashingRoot(),_hasher,getConfig().plotRoot());
			Printer.printHashDistPlot(3,getConfig().hashingRoot(),_hasher,getConfig().plotRoot());

			Printer.gnuplot2DimHashPoints(getConfig().hashingRoot(),_hasher,1,3,getConfig().plotRoot(),getConfig().maxDim());
			Printer.gnuplot2DimHashPoints(getConfig().hashingRoot(),_hasher,1,2,getConfig().plotRoot(),getConfig().maxDim());
			Printer.gnuplot2DimHashPoints(getConfig().hashingRoot(),_hasher,2,3,getConfig().plotRoot(),getConfig().maxDim());

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean exists() {
		return _datFile.exists();
	}

	@Override
	public String summary() {
		StringBuilder sb = new StringBuilder();
		
		sb.append("total time: ").append(_totalTime).append("\n");
		
		return sb.toString();
	}


	
}
