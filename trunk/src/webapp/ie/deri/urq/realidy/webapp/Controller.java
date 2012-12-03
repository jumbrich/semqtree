 package ie.deri.urq.realidy.webapp;

import ie.deri.urq.realidy.index.SemQTree;
import ie.deri.urq.realidy.insert.InsertCallback;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Logger;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.ontologycentral.ldspider.Crawler;
import com.ontologycentral.ldspider.hooks.fetch.FetchFilterDeny;

import de.ilmenau.datasum.index.AbstractIndex;

public class Controller extends HttpServlet {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private final static org.apache.log4j.Logger  log = org.apache.log4j.Logger.getLogger(Controller.class);
    	
    
    public static String PARAM_MAX_BUCKETS = "maxBuckets";
    public static String PARAM_HASHING = "hash";
    public static String PARAM_FANOUT = "fanout";
    public static String PARAM_MAX_DIM = "maxDim";
    public static String PARAM_STOREDETAILEDCOUNT = "storeDetailedCount";
    public static String PARAM_LABEL = "label";
    public static String ACION_CREATE = "create";
    public static String PARAM_THREADS ="threads"; 
    public static String PARAM_HOPS ="hops";
    public static String PARAM_TOP_K ="topK";
    public static String ACION_INSERT="insert";
    public static String ACION_INFO="info";
    public static String ACTION_QUERY="query";
    public static String ACTION_DELETE="delete";
    public static String ACTION_EVALUATE="evaluate";
    public static String PARAM_SEEDS = "seeds";
    public static String PARAM_QUERY = " query";
    
    private final static boolean DEBUG = Boolean.TRUE;
    public static final String CTX_CONTROLLER = "controller";
    public static final String CTX_NOTIFY = "notify";
    public static final String CTX_ERROR = "error";
    public static final String PARAM_FORMAT = "format";
    public static final String CTX_ERROR_URL = "errorURL";
    public static final String PARAM_VISITOR = "visitor";
    public static final Object VISITOR_BGP = "bgp";
    public static final Object VISITOR_JOIN = "join";
    public static final Object VISITOR_HYBRID = "hybrid";
    private ServletContext _ctx;
    private Object _indices;
    private File _indicesDir;
    
    /** private variables **/
   
    @Override
    public void init(ServletConfig config) throws ServletException {
	//initialise here all objects needed for the processing
	_ctx = config.getServletContext();
	_indicesDir = new File(_ctx.getInitParameter("DATA_DIR"));
	
	_ctx.setAttribute(CTX_CONTROLLER, this);
	log.info("Initialised the FrontController");
    }

    public File getIndexDir(){
	return _indicesDir;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
    throws ServletException, IOException {
	/** TODO **/
	System.out.println("GET");
	doProcess(req, resp);
    }


    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
    throws ServletException, IOException  {
	System.out.println("POST");
	
	
	doProcess(req, resp);
	
	
    }

    private void doProcess(HttpServletRequest request, HttpServletResponse response)
    throws IOException, ServletException {
	
	String para = debugParameters(request);
	
	String [] reqURI=request.getRequestURI().split("/");
	String action = reqURI[reqURI.length-1];
	System.out.println("ACTION: "+action);
	
	String msg = "ACTION: "+action;
	msg+="\nSubmitted parameters:\n"+para;
	long start = System.currentTimeMillis();
	String notify = null;
	try{
	    if(action.equals(ACION_CREATE)){
		notify =create(request,response);
	    }else if(action.equals(ACION_INSERT)){
		notify =insert(request,response);
	    }else if(action.equals(ACTION_QUERY)){
		notify =query(request,response);
	    }else if(action.equals(ACION_INFO)){
		notify =info(request,response);
	    }
	    else if(action.equals(ACTION_DELETE)){
		notify =delete(request,response);
	    }
	    else{
		_ctx.getRequestDispatcher("/index.jsp").forward(request, response);
	    }
	    long end = System.currentTimeMillis();
	    msg+="\n TIME ELAPSED: "+(end-start)+" ms!";
	    _ctx.setAttribute(CTX_ERROR_URL, request.getRequestURI()+request.getQueryString());
	    msg+="\nMSG: "+notify;
	    _ctx.setAttribute(CTX_NOTIFY, msg);
	    
	   if(notify!=null)
	       response.sendRedirect(request.getContextPath()+"/"+response.encodeRedirectURL("notify.jsp"));
	}catch(Exception e){
	    e.printStackTrace();
	    _ctx.setAttribute(CTX_ERROR_URL, request.getRequestURI()+request.getQueryString());
	    _ctx.setAttribute(CTX_ERROR, e);
	    response.sendRedirect(request.getContextPath()+"/"+response.encodeRedirectURL("error.jsp"));
	}
    }
    
    private String delete(HttpServletRequest request, HttpServletResponse response) throws IOException {
	String label = request.getParameter(PARAM_LABEL);
	new File(_indicesDir,label).delete();
	boolean deleted = new File(_indicesDir,label).exists();
	if(deleted){
	    response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "could not delete index file "+label+". Please check your servlet container policies.");
	    return null;
	}else return "successfully deleted "+label;
    }

    private String info(HttpServletRequest request, HttpServletResponse response) throws IOException {
	String label = request.getParameter(PARAM_LABEL);
	AbstractIndex idx= AbstractIndex.loadIndex(new File(_indicesDir,label));
	response.getWriter().println(idx.info());
	return null;
    }

    private String query(HttpServletRequest request, HttpServletResponse response) throws IOException {
	String queryString = request.getParameter(PARAM_QUERY);
	String label = request.getParameter(PARAM_LABEL);
	String format = request.getParameter(PARAM_FORMAT);
	Query query = QueryFactory.create(queryString);
	AbstractIndex idx= AbstractIndex.loadIndex(new File(_indicesDir,label));
//      StageGeneratorQTree msg = new StageGeneratorQTree(null);
//      ARQ.getContext().set(ARQ.stageGenerator, msg) ;
      
	System.out.println(Logger.getLogger("com.ontologycentral.ldspider.*"));
//     
//	QTreeQueryEngine.register();
//	QTreeQueryEngine.setQTree(idx);
//	String thread = request.getParameter(PARAM_THREADS);
//	int i_threads = Integer.valueOf(thread);
//	String topK = request.getParameter(PARAM_TOP_K);
//	int i_topK = Integer.valueOf(topK);
//	
////	QTreeQueryEngine.setThreads(i_threads);
////	QTreeQueryEngine.setTopK(i_topK);
//	
//	QTreeQueryEngine.register();
//	
//	QueryBenchmark bench = null;
//	if(request.getParameter(ACTION_QUERY).equals(ACTION_EVALUATE)){
//	    bench = new QueryBenchmark();
//	}
//	    
//	QTreeOpVisitor visitor=null;
////	if(request.getParameter(PARAM_VISITOR).equals(VISITOR_BGP)){
////	    visitor = new BGPOpVisitor(idx,i_topK,bench);
////	}else if(request.getParameter(PARAM_VISITOR).equals(VISITOR_JOIN)){
////	    visitor = new JoinOpVisitor(idx,i_topK,bench);
////	}else if(request.getParameter(PARAM_VISITOR).equals(VISITOR_HYBRID)){
////	    visitor = new HybridOpVisitor(idx,i_topK,bench);
////	}
//	
//	QTreeDataset ds =  new QTreeDataset(i_threads, visitor, bench);
//	QueryExecution engine = QueryExecutionFactory.create(query,ds);
//	
//	try {
//	    System.out.println("EXECUTE SELECT.");
//	  ResultSet results = engine.execSelect() ;
//	  /*
//	  Predefined values are "RDF/XML", "RDF/XML-ABBREV", "N-TRIPLE", "TURTLE", (and "TTL") and "N3". The default value, represented by null, is "RDF/XML".
//	  */
////	 System.out.println("Dataset contains:"+engine.getDataset().asDatasetGraph().size()); 
//	  if(request.getParameter(ACTION_QUERY).equals(ACTION_EVALUATE)){
//	      bench = ds.getBenchmark();
////	      System.out.println("Bench: "+bench);
//	      bench.setQuery(queryString);
//	      response.getWriter().println(bench.toString());
//	      
//	  }
//	  else{
//	      ResultSetFormatter.outputAsRDF(response.getOutputStream(), format, results);
//	  }
//	} finally {
//		engine.close() ;
//	}
	return null;
    }

    private String insert(HttpServletRequest request, HttpServletResponse response) {
	String index = request.getParameter(PARAM_LABEL);
	String hops = request.getParameter(PARAM_HOPS);
	int i_hops = Integer.valueOf(hops);
	String thread = request.getParameter(PARAM_THREADS);
	int i_threads = Integer.valueOf(thread);
	List<URI> seeds = new ArrayList<URI>();
	Scanner s = new Scanner(request.getParameter(PARAM_SEEDS));
	while(s.hasNextLine()){
	    try {
		seeds.add(new URI(s.nextLine().trim()));
	    } catch (URISyntaxException e) {
		log.info(e.getClass().getSimpleName()+": "+e.getMessage());
	    }
	}
 	SemQTree idx = SemQTree.loadIndex(new File(_indicesDir,index));
	
	InsertCallback cb = new InsertCallback(idx);
	Crawler crawler = new Crawler();
	
	
	crawler.setOutputCallback(cb);
	crawler.setFetchFilter(new FetchFilterDeny());
//	crawler.evaluate(seeds, i_hops, i_threads);
	
	crawler.close();
	
	idx.serialiseIndexToFile(new File(_indicesDir,index));
	return "inserted "+seeds.size()+" sources into index "+index;
    }

    private String create(HttpServletRequest request, HttpServletResponse response) {
	
	String maxBuckets = request.getParameter(PARAM_MAX_BUCKETS);
	int i_MB = Integer.valueOf(maxBuckets);
	String fanout = request.getParameter(PARAM_FANOUT);
	int i_fanout = Integer.valueOf(fanout);
	String hash = request.getParameter(PARAM_HASHING);
	String maxDim = request.getParameter(PARAM_MAX_DIM);
	int i_maxDim = Integer.valueOf(maxDim);
	
	boolean storeDetailedCount = request.getParameter(PARAM_STOREDETAILEDCOUNT)!=null;
	
	String label = request.getParameter(PARAM_LABEL);
	
	SemQTree idx = SemQTree.createSemQTreeSingleQTreeIndex(hash,i_MB,i_fanout,0,i_maxDim, storeDetailedCount); 
	File out = _indicesDir;
	if(label.trim().length() == 0){
	    label = idx.getConfigurationLabel();
	}else{
	    label = label.replaceAll(" ", "_");    
	}
	out = new File(_indicesDir,label);
	int count= 1;
	while(out.exists()){
	    out = new File(_indicesDir,label+"."+count);
	    count++;
	}
	idx.serialiseIndexToFile(out);
	label = out.getName();
	return "create new index with label "+label;
    }


    private String debugParameters(HttpServletRequest req) {
	
	String para = "requestedURI: "+req.getRequestURI()+"\n"+ 
	    "Session "+req.getSession().getId()+"\n";
	Enumeration paraNames = req.getParameterNames();
	para+="Parameters\n";
	while(paraNames.hasMoreElements()){
	    Object name = paraNames.nextElement();
	    para+=" "+name+": "+req.getParameter(name.toString())+"\n";
	}
	para+="Session beans\n";
	Enumeration sessionParaNames = req.getSession().getAttributeNames();
	while(sessionParaNames.hasMoreElements()){
	    Object name = sessionParaNames.nextElement();
	    para+=" "+name+": "+req.getSession().getAttribute((String)name)+"\n";
	}
	log.info("[DEBUG]\n"+para);
	return para;
    }
}