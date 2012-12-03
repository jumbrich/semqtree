package de.ilmenau.datasum.index;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;



import de.ilmenau.datasum.exception.QTreeException;

public abstract class AbstractIndex implements Serializable {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;


	private static final Logger log = Logger.getLogger(AbstractIndex.class.getName());
     
    
    public static final int VARIABLE = -234;
    
    protected long _totalSerialisedTreeSize;
    
    protected int[] dimSpecMin = {0,0,0}; //int[] minValues = {0,0,0};
    protected int[] dimSpecMax = {3000,3000,3000}; //int[] maxValues = {1,1,875};
    
    protected int noOfStmts;
    
    private final String hasher;


	protected boolean _debug = false;
    
    public void enableDebugMode(boolean enable){
		_debug = enable;
	}
    
    public AbstractIndex(String hasher, int [] dimSpecMin, int [] dimSpecMax) {
    	this.dimSpecMin = dimSpecMin;
    	this.dimSpecMax = dimSpecMax;
    	this.hasher = hasher;
    }
    
    public boolean insertStatement(final int[] stmt, final String source){
	try {
	   addStatment(stmt,source);
	    this.noOfStmts++;
	    return Boolean.TRUE;
	} catch (Exception e) {
	    log.log(Level.SEVERE,"During insert of statement into a index", e);
	    return Boolean.FALSE;
	}
    }
    
    abstract protected void addStatment(final int[] stmt, final String source) throws Exception;
    abstract public Collection<String> getRelevantSourcesForQuery(final int[][] bgpsHashCoordinates, final int[][] join) throws QTreeException ;
    abstract public QueryResultEstimation evaluateQuery(final int[][] bgpsHashCoordinates, final int[][] join, boolean determineResultingBuckets, boolean useParentJoinSpace) throws QTreeException;
    abstract public int getNoOfSources();

    /**
     * 
     * @param index - location of the serialised index
     * @return - the deserialsed instance of the index - if a failure occurred a <code>null</code> reference is returned
     */
    public static AbstractIndex loadIndex(File index){
	//TODO
	log.log(Level.FINE,"Request for deserialising a from file "+index.getAbsolutePath());
	ObjectInputStream ois;
	try {
	    ois = new ObjectInputStream(new FileInputStream(index));
	    AbstractIndex idx = (AbstractIndex) ois.readObject();
	    return idx;

	} catch (Exception e) {
	    log.log(Level.SEVERE,"Request for deserialisation of SingleQTreeIndex failed", e);
	    return null;
	}
    }

    /**
     * Serialise an instance of a SingelQTreeIndex to the specified file
     * @param onDiskFile - the file to store the serialised version on disk
     * @return - true if the instance could be successfully serialised - false otherwise
     */
    public boolean serialiseQTreeToFile( File onDiskFile) {
        log.log(Level.FINE,"Request for serialising a SingleQTreeIndex to file "+onDiskFile.getAbsolutePath());
    
        long start = System.currentTimeMillis();
    
        if(onDiskFile.getParentFile()!=null && !onDiskFile.getParentFile().exists()){
            onDiskFile.getParentFile().mkdirs();
        }
    
        ObjectOutputStream oos;
        try {
            oos = new ObjectOutputStream(new FileOutputStream(onDiskFile));
            oos.writeObject(this);
            oos.close();
            _totalSerialisedTreeSize += onDiskFile.length();
            long end = System.currentTimeMillis();
    
            log.info("Serialised the QTree in "+(end-start)+" ms. on-disk size: "+onDiskFile.length()/1024+" KBytes to "+onDiskFile);
            return Boolean.TRUE;
        } catch (Exception e) {
            log.log(Level.SEVERE,"During serialisation of SingleQTreeIndex", e);
        }
        return Boolean.FALSE;
    
    }

  
    /**
     * @return the dimSpecMin
     */
    public int[] getDimSpecMin() {
	return dimSpecMin;
    }

    /**
     * @return the dimSpecMax
     */
    public int[] getDimSpecMax() {
	return dimSpecMax;
    }
    

    abstract public String info();

    public int getNoOfStmts() {
	return noOfStmts;
    }

    public String getHasher() {
	return this.hasher;
    }

	abstract public Vector<IntersectionInformation> getAllBucketsInQuerySpace(
			QuerySpace querySpace);

	abstract public boolean getStoreDetailedCount();

	public String getVersionType() {
		return versionType();
	}
	
	protected abstract String versionType();
}
