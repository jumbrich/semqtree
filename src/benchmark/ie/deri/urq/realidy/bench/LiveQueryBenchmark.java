package ie.deri.urq.realidy.bench;

import ie.deri.urq.realidy.bench.utils.BenchmarkConfig;
import ie.deri.urq.realidy.bench.utils.Formater;
import ie.deri.urq.realidy.hashing.QTreeHashing;
import ie.deri.urq.realidy.index.SemQTree;
import ie.deri.urq.realidy.query.arq.QueryExecutor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import com.hp.hpl.jena.query.ResultSet;

import de.ilmenau.datasum.index.QueryResultEstimation;

public class LiveQueryBenchmark extends Benchmark {

	private SemQTree sqt;

	public LiveQueryBenchmark(BenchmarkConfig config, String version,
			QTreeHashing hasher) {
		super(config, version, hasher);
		sqt = SemQTree.loadIndex(config.getIndexFile(version,hasher.getHasherName()));
	}

	@Override
	public void benchmark() {
		getConfig().jenaEvalRoot().mkdirs();
		long start = System.currentTimeMillis();
		try {
			FileOutputStream fis = new FileOutputStream(getConfig().jenaEvalQueryFile(_version,_hasher));
			fis.write(("#queryNo totaltime jointime ranktime buckets sources realStmt qtreeAllStmt top10Stmt top50Stmt top100Stmt top200Stmt\n").getBytes());

			File [] queries = getConfig().queryRoot().listFiles();
			for(File f: queries){
				if(!f.isFile())continue;
				try {
						String name = f.getName();
						String numeric = name.substring(name.lastIndexOf("qre.")+4,name.lastIndexOf(".ser"));
						int queryCount =  Integer.valueOf(numeric);
						QueryResultEstimation qre = deserialise(f);
						evaluate(qre, queryCount, fis, 10, 20);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			fis.flush();fis.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void evaluate(QueryResultEstimation qre, int queryCount,
			FileOutputStream fis,int thread, int topK) {
		String queryString = qre.getQueryString();
		long start = System.currentTimeMillis();
		QueryExecutor executor = new QueryExecutor(sqt, false);
		ResultSet set = executor.executeQuery(queryString,thread,topK,null);
		long end = System.currentTimeMillis();
		int count = 0;
		while(set.hasNext()){
			set.next();count++;
		}
		log("Q["+queryCount+"] Result "+count+" in "+Formater.readableTime(end-start));
	}

	private  QueryResultEstimation deserialise(File serQueryFile) throws IOException, ClassNotFoundException {
		log("Deserialise "+serQueryFile);
		FileInputStream fis = new FileInputStream(serQueryFile);
		ObjectInputStream ois = new ObjectInputStream(fis);
		Object o = ois.readObject();
		ois.close();
		fis.close();

		return (QueryResultEstimation) o;
	}

	@Override
	public void benchmarkPlot() {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean exists() {
		// TODO Auto-generated method stub
		return false;
	}

}
