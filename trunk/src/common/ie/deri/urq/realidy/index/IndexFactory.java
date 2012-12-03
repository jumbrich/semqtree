package ie.deri.urq.realidy.index;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ie.deri.urq.realidy.bench.utils.BenchmarkConfig;
import ie.deri.urq.realidy.hashing.QTreeHashing;

public class IndexFactory {
	private final static Logger log = LoggerFactory.getLogger(IndexFactory.class);
	
		public static IndexInterface createIndex(String version,
			QTreeHashing hasher, BenchmarkConfig config) {
			return createIndex(version, hasher, config.maxBuckets(),config.maxFanout(),config.maxDim(),config.storeDetailedCount());
		}
		public static IndexInterface createIndex(String version,
					QTreeHashing hasher, int maxBuckets, int fanout,int maxDim, boolean storeDetailedCount) {		
			if(version.equals("qtree")){
				return SemQTreeFactory.create(version, hasher, maxBuckets, fanout, maxDim, storeDetailedCount);
			}else if(version.equals("histo")){
				return SemQTreeFactory.create(version, hasher, maxBuckets, fanout, maxDim, storeDetailedCount);
			}else if(version.equals("schema")){
				return new SchemaIndex();
			}else if(version.equals("inv-uri")){
				return new InvertedURIIndex();
			}else{
				System.err.println("Dooh, version \""+version+"\" not known.");
			}
			
		return null;
	}

		public static IndexInterface loadIndex(File indexFile) {
			if(indexFile.getName().startsWith("qtree")){
				return SemQTree.loadIndex(indexFile);
			}else if(indexFile.getName().startsWith("histo")){
				return SemQTree.loadIndex(indexFile);
			}else if(indexFile.getName().startsWith("schema")){
				return deserialiseIndex(indexFile);
			}else if(indexFile.getName().startsWith("inv-uri")){
				return deserialiseIndex(indexFile);
			}
			return null;
		}

		private static IndexInterface deserialiseIndex(File indexFile) {
			ObjectInputStream ois;
			try {
				long start= System.currentTimeMillis();
				ois = new ObjectInputStream(new FileInputStream(indexFile));
				IndexInterface idx = (IndexInterface)  ois.readObject();

				log.info("Loaded index from location {} with size {} KBytes in {} ms.", new Object[]{indexFile,indexFile.length()/1024,(System.currentTimeMillis()-start)});
				return idx;
			} catch (Exception e) {
				log.warn("Request for deserialisation of SingleQTreeIndex failed", e);
				return null;
			}
		}
}
