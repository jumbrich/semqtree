import ie.deri.urq.realidy.cli.AllInOne;
import junit.framework.TestCase;


public class AllInOneTEST extends TestCase{

	public void testAllInOne(){
		String [] args = {"-od","bench_m50K_b10K", "-hash","adv_ns","adv","adv_int","adv_int2","-m","50000","-local","/Users/juum/Data/datasets/foaf_hop4.nq.gz","-q","test/resources/queries_path_50_3.txt"};
		AllInOne a = new AllInOne();
		a.run(args);
	}
}
