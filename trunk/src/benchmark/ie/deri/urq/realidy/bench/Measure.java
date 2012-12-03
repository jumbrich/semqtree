package ie.deri.urq.realidy.bench;

import ie.deri.urq.realidy.insert.Indexer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.parser.ParseException;

public class Measure implements Callback{ 
	
	

    private long _tic;
    private File _benchDir;
    private Callback _childCB;
    private long _start;
    int _counter=0,_lastCounter=0;
    long _lastTic=0;
    private boolean _ticIsCount;
    private FileWriter _fw;
    private Runtime _runtime;
	private long overAllTime;

    public Measure(Callback cb, File outputFile, Long tic, boolean ticIsCount) throws IOException {
    	outputFile.getParentFile().mkdirs();
    	_childCB = cb;
    	_tic = tic;
    	_ticIsCount = ticIsCount;
    	_fw = new FileWriter(outputFile);
    	_runtime = Runtime.getRuntime();
    	_start = System.currentTimeMillis();
    	_lastTic = System.currentTimeMillis();
    }

    public void endDocument() {
    	_childCB.endDocument();
    	try {
    		if(_fw !=null)
    			_fw.close();
    		_fw = null;
    	} catch (IOException e) {
    	    e.printStackTrace();
    	}
        

    }
    public void processStatement(Node[] stmt) {
    	long start = System.currentTimeMillis();
		_childCB.processStatement(stmt);
		overAllTime += (System.currentTimeMillis()-start);
		_counter++;
	
		try{
			if(_ticIsCount && _counter % _tic==0){
				//
				updateTime();
				_lastTic = System.currentTimeMillis();
			}else if(!_ticIsCount && System.currentTimeMillis()-_lastTic > _tic){
	    		update(_counter, _counter - _lastCounter);
				_lastCounter = _counter;
				_lastTic = System.currentTimeMillis();
	    }

	}catch(Exception e){e.printStackTrace();}

    }

    private void updateTime() throws IOException {
    	long time = System.currentTimeMillis();
    	long maxMemory =_runtime.maxMemory();
    	long allocatedMemory =_runtime.totalMemory();
    	long freeMemory =_runtime.freeMemory();
    	/*
    	 * 	count time diffTime used free
    	 */
    	
    	_fw.write(""+_counter+" "+(time-_start)+" "+(time-_lastTic)+" "+ (allocatedMemory-freeMemory)+" "+(freeMemory+maxMemory-allocatedMemory)+"\n");
    	_fw.flush();
    }

    public void update(int counter, int diff) throws IOException{
    	/*
    	 * time count diff used free
    	 */
    	long time = System.currentTimeMillis();
    	long maxMemory =_runtime.maxMemory();
    	long allocatedMemory =_runtime.totalMemory();
    	long freeMemory =_runtime.freeMemory();

    	_fw.write(""+(time-_start)+" "+_counter+" "+(_counter-_lastCounter)+" " + (allocatedMemory-freeMemory)+" "+(freeMemory+maxMemory-allocatedMemory)+"\n");
    }
    public void startDocument() {
    	_childCB.startDocument();
    	_start = System.currentTimeMillis();
    	_lastTic = System.currentTimeMillis();
    }
    
	public void indexLocal(File file) throws ParseException, IOException {
		Indexer.insertFromNXZ(file, this);
		
	}
	public Long getRealTime() {
		// TODO Auto-generated method stub
		return overAllTime;
	}
	
	public int getCounter(){
		return _counter;
	}
}