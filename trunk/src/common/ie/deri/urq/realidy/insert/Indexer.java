package ie.deri.urq.realidy.insert;

import ie.deri.urq.realidy.index.SemQTree;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collection;
import java.util.zip.GZIPInputStream;

import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;
import org.semanticweb.yars.util.Callbacks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ontologycentral.ldspider.Crawler;
import com.ontologycentral.ldspider.frontier.Frontier;
import com.ontologycentral.ldspider.frontier.RankedFrontier;
import com.ontologycentral.ldspider.hooks.fetch.FetchFilterDeny;
import com.ontologycentral.ldspider.hooks.fetch.FetchFilterRdfXml;
import com.ontologycentral.ldspider.hooks.links.LinkFilterDefault;

public class Indexer {
private final static Logger log = LoggerFactory.getLogger(Indexer.class);
	public static void insertFromWeb(Collection<URI> seeds, Callback cb, int threads, int hops){
		Frontier frontier = new RankedFrontier();
		frontier.addAll(seeds);
		insertFromWeb(frontier, cb, threads, hops);
	}

	public static void insertFromWeb(URI uri, SemQTree sqt,  int threads, int hops){
		InsertCallback cb = new InsertCallback(sqt);
		Frontier frontier = new RankedFrontier();
		frontier.add(uri);
		insertFromWeb(frontier, cb, threads, hops);
	}

	public static void insertFromWeb(URI uri, Callback cb, int threads, int hops){
		Frontier frontier = new RankedFrontier();
		frontier.add(uri);
		insertFromWeb(frontier, cb, threads, hops);
	}

	public static void insertFromWeb(Frontier frontier, SemQTree sqt, int threads, int hops){
		InsertCallback cb = new InsertCallback(sqt);
		insertFromWeb(frontier, cb, threads, hops);
	}

	public static void insertFromWeb(Frontier frontier, Callback cb, int threads, int hops){
		log.info("Starting web crawl with "+threads+" threads and "+hops+" hops");
		Crawler crawler = new Crawler(threads);
		crawler.setOutputCallback(cb);
		crawler.setFetchFilter(new FetchFilterRdfXml());
		crawler.setLinkFilter(new LinkFilterDefault(frontier));
		crawler.evaluateBreadthFirst(frontier, hops, -1);
		crawler.close();
	}

	public static void insertFromNXZ(File nxzFile, SemQTree index) throws ParseException, IOException{
		InsertCallback cb = new InsertCallback(index);
		insertFromNXZ(nxzFile, cb);
	}

	public static void insertFromNXZ(File nxzFile, Callback cb) throws ParseException, IOException{
		System.out.println("Inserting content from "+nxzFile);
		InputStream is = new FileInputStream(nxzFile);    
		if(nxzFile.getName().endsWith(".gz")){
			is = new GZIPInputStream(is);
		}
		NxParser nxp = new NxParser(is,cb);

		is.close();
	}
}