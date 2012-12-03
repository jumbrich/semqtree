package main;

import java.io.File;

public class CONSTANTS {

    public static final File TESTDIR = new File("tmp.test.dir");

    public static void cleanup() {
	deleteFolder(TESTDIR);
	
	
    }

    private static void deleteFolder(File dir) {
	File [] f = dir.listFiles();
	for(File f1: f){
	    if(f1.isDirectory()) deleteFolder(f1);
	    f1.delete();
	}
    }
    
    public static void setProxy(String host, int port){
	System.setProperty("http.proxyHost",host);
	System.setProperty("http.proxyHost",""+port);
    }

}
