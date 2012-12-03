package ie.deri.urq.realidy.query.arq;

import ie.deri.urq.realidy.index.SemQTree;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import at.jku.multiarq.OpThreadedExecutor;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.Plan;
import com.hp.hpl.jena.sparql.engine.QueryEngineFactory;
import com.hp.hpl.jena.sparql.engine.QueryEngineRegistry;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.main.OpExecutor;
import com.hp.hpl.jena.sparql.engine.main.QC;
import com.hp.hpl.jena.sparql.engine.main.QueryEngineMain;
import com.hp.hpl.jena.sparql.util.Context;
import com.hp.hpl.jena.sparql.util.Symbol;
import com.ontologycentral.ldspider.Crawler;
import com.ontologycentral.ldspider.CrawlerConstants;
import com.ontologycentral.ldspider.frontier.BasicFrontier;
import com.ontologycentral.ldspider.frontier.Frontier;
import com.ontologycentral.ldspider.hooks.error.ErrorHandler;
import com.ontologycentral.ldspider.hooks.error.ErrorHandlerLogger;
import com.ontologycentral.ldspider.hooks.fetch.FetchFilterRdfXml;
import com.ontologycentral.ldspider.hooks.links.LinkFilterDummy;

import de.ilmenau.datasum.exception.QTreeException;

public class QTreeQueryEngine extends QueryEngineMain {
	public static final Symbol BENCHMARK = Symbol.create("benchmark");
	private final QueryParser _qp = new QueryParser();

	
	private Crawler _crawler;
	private QueryBenchmark _bench;
	private SemQTree _sqt;
	private int _threads;

	private int _topK;
	private boolean _enableDLApproach;
	private static final Logger log = LoggerFactory.getLogger(QTreeQueryEngine.class);

	public QTreeQueryEngine(Op op, DatasetGraph dsg, Binding input,
			Context context) {
		super(op, dsg, input, context);
		init((QTreeDataSourceGraph)dsg);


	}

	private void init(QTreeDataSourceGraph dataset) {
		_threads = dataset.getThreads();
		_crawler = new Crawler(_threads);
		_topK = dataset.getTopK();
		_bench = dataset.getBenchmark();
		_sqt = dataset.getSemQTree();
		_enableDLApproach = dataset.enableDLApproach();

	}

	public QTreeQueryEngine(Query query, DatasetGraph dsg, Binding initial,
			Context context) {
		super(query,dsg,initial, context);
		init((QTreeDataSourceGraph)dsg);
	}


	@Override
	public QueryIterator eval(Op op, DatasetGraph dsg, Binding input,
			Context context) {
		//	_bench = new QueryBenchmark();
		System.out.println("eval");
		if(_bench!=null)
			_bench.setTime("arq.eval",System.currentTimeMillis(),true);
		QueryIterator iter = super.eval(op, dsg, input, context);
		if(_bench!=null)
			_bench.setTime("arq.eval",System.currentTimeMillis(),false);

		return iter;
	}

	@Override
	protected Op modifyOp(Op op) {
		//magic happens here
		if(_bench!=null){
			_bench.setTime("arq.op",System.currentTimeMillis(),true);
			_bench.setTime("qtree.select",System.currentTimeMillis(),true);
		}
		//	BGPOpVisitor visitor = new BGPOpVisitor(_index,_topK,_bench);
		Collection<String> sources = null;
		try {
			sources = _sqt.getRelevantSourcesForQuery(op);
		} catch (QTreeException e) {
			;
		}
		if(_bench!=null){
			_bench.setTime("qtree.select",System.currentTimeMillis(),false);
			_bench.setTime("fetch.src",System.currentTimeMillis(),true);
		}
		if(_enableDLApproach&&( sources==null || sources.size()==0)){
			QC.setFactory(context, OpThreadedExecutor.getFactory());
			log .info("Switching to Direct Lookup Approach.");
		}else{
			fetchSources(sources,_bench,_qp.getBGPPattern(op));
		}
		if(_bench!=null){
			_bench.setTime("fetch.src",System.currentTimeMillis(),false);	
			_bench.setTime("arq.op",System.currentTimeMillis(),false);
		}
		return op;
	}


	private void fetchSources(Collection<String> sources, QueryBenchmark bench, BasicPattern basicPattern) {
		if(sources == null) return;
		//transform seeds into URIs
		Frontier frontier = new BasicFrontier();
		log.info("Start fetching from {} sources with topk: {}",new Object[]{sources.size(),_topK});
		int count = 1;
		for(String s : sources){
			if(count>_topK) break;
			try {
				frontier.add(new URI(s));
				count++;
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
		}
		
		ErrorHandler eh = new ErrorHandlerLogger(null, null);
		frontier.setErrorHandler(eh);
		frontier.setBlacklist(CrawlerConstants.BLACKLIST);
		
		_crawler.setFetchFilter(new FetchFilterRdfXml());
		BGPMatcherCallback bgpm = new BGPMatcherCallback(dataset,basicPattern,bench);
		_crawler.setOutputCallback(bgpm);
		_crawler.setLinkFilter(new LinkFilterDummy());
		_crawler.evaluateBreadthFirst(frontier, 3, CrawlerConstants.DEFAULT_NB_URIS);

		_crawler.close();
		log.info("Size of the dataset after fetch {}",dataset.getDefaultGraph().size());
	}

	static public QueryEngineFactory getFactory() {
		return factory;
	}

	static public void register() {
		QueryEngineRegistry.addFactory(factory);
	}

	static public void unregister() {
		QueryEngineRegistry.removeFactory(factory);
	}

	/**
	 * factory
	 */
	private static QueryEngineFactory factory = new QueryEngineFactory() {

		public boolean accept(Query query, DatasetGraph dsg, Context context) {
			return dsg instanceof QTreeDataSourceGraph;
		}

		public Plan create(Query query, DatasetGraph dsg, Binding initial, Context context) {
			QTreeQueryEngine engine = new QTreeQueryEngine(query, dsg, initial, context);
			return engine.getPlan();
		}

		public boolean accept(Op op, DatasetGraph dsg, Context context) {
			return dsg instanceof QTreeDataSourceGraph;
		}

		public Plan create(Op op, DatasetGraph dsg, Binding initial, Context context) {
			QTreeQueryEngine engine = new QTreeQueryEngine(op, dsg, initial, context);
			return engine.getPlan();
		}

	};
}

class DefaultOpExecutor extends OpExecutor{

	protected DefaultOpExecutor(ExecutionContext execCxt) {
		super(execCxt);
	}
	
}