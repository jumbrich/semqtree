package main;

import ie.deri.urq.realidy.index.SemQTreeTEST;
import indexer.IndexerTest;
import junit.framework.Test;
import junit.framework.TestSuite;


public class AllTestCases extends TestSuite{

    
    
    public static Test suite(){
	TestSuite suite = new TestSuite("All Tests");
	suite.addTestSuite(Setup.class);
	
	suite.addTestSuite(SemQTreeTEST.class);
	suite.addTestSuite(IndexerTest.class);
	
	suite.addTestSuite(TearDown.class);
	return suite;
    } 
}
