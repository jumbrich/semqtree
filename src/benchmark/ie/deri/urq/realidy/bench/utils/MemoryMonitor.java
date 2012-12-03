package ie.deri.urq.realidy.bench.utils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class MemoryMonitor extends Thread {

	private FileWriter _fw;
	private long _ticTime;
	private boolean _stop = false;
	private Runtime _runtime;
	private long _start;
	private long free;

	/**
	 * 
	 * @param ticTime - in milliseconds
	 * @param logFile - if specified, each tick value will be logged into as a tuple [tic, value]
	 * @throws IOException 
	 */
	public MemoryMonitor(long ticTime, File logFile) throws IOException {
		_ticTime = ticTime;
		if(logFile !=null){
			_fw = new FileWriter(logFile);
		}
		_runtime = Runtime.getRuntime();
		_start = System.currentTimeMillis();
	}
	@Override
	public void run() {
		_start = System.currentTimeMillis();
		while(!_stop ){
			try {
				System.out.println(monitorMemory());
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			try {
				Thread.sleep(_ticTime);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	public Long getFreeBytes(){
		Long maxMemory =_runtime.maxMemory();
		Long allocatedMemory =_runtime.totalMemory();
		Long freeMemory =_runtime.freeMemory();
		return free = freeMemory+maxMemory-allocatedMemory;
	}

	public String monitorMemory() throws IOException {
		Long maxMemory =_runtime.maxMemory();
		Long allocatedMemory =_runtime.totalMemory();
		Long freeMemory =_runtime.freeMemory();

		StringBuilder sb = new StringBuilder("T[");
		sb.append(Formater.getLine(Formater.readableTime(System.currentTimeMillis()-_start), 12));
		sb.append("]-MEM[ max:");
		sb.append(Formater.getLine(Formater.getMemory(maxMemory),10));
		sb.append(" alc:");
		sb.append(Formater.getLine(Formater.getMemory(allocatedMemory),10));
		sb.append(" used:");
		sb.append(Formater.getLine(Formater.getMemory(allocatedMemory-freeMemory),10));
		sb.append(" free:");
		free = freeMemory+maxMemory-allocatedMemory;
		sb.append(Formater.getLine(Formater.getMemory(freeMemory+maxMemory-allocatedMemory),10));
		sb.append("]");
		if(_fw !=null){
			_fw.write(System.currentTimeMillis()-_start+" "+(allocatedMemory-freeMemory)+" "+(freeMemory+maxMemory-allocatedMemory) + "\n");
		}
		return sb.toString();
		
	}
	public void stopMonitor(){
		this.interrupt();
		_stop = true;
		if(_fw != null)
			try {
				_fw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
	}
}
