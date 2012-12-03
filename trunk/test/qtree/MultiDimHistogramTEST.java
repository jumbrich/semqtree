package qtree;

import java.util.Vector;

import junit.framework.TestCase;

import de.ilmenau.datasum.index.histogram.MultiDimHistogram;

public class MultiDimHistogramTEST extends TestCase{

	
	public void testMain() throws Exception {
		MultiDimHistogram h  = new MultiDimHistogram(3, 00, new int[]{0,0,0}, new int[]{1000000,1000000,1000000}, "1", "0",new Vector(), true);
	}
}
