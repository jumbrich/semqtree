package main;

import junit.framework.TestCase;


public class TearDown  extends TestCase{

    @Override
    protected void tearDown() throws Exception {
        // TODO Auto-generated method stub
        super.tearDown();
        CONSTANTS.cleanup();
    }
    
    public void testVoid() {
	;
    } 
}
