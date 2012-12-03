package ie.deri.urq.realidy.bench.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Scanner;

import org.apache.commons.math.stat.descriptive.SummaryStatistics;

public class SummariseQueryEvaluation {


	public  void benchmark(File logfile) throws IOException {
		Scanner s = new Scanner(logfile);

		//"QueryNumber realSrc realStmt estSrc estTime queryEvalTime percTop10 timeTop10 percTop50 timeTop50 percTop100 timeTop100 percTop200 timeTop200 maxK swcSrc swcStmt";
		//"#queryNo totaltime jointime ranktime buckets sources realStmt qtreeAllStmt top10Stmt top50Stmt top100Stmt top200Stmt

		SummaryStatistics  totalTime = new SummaryStatistics();
		SummaryStatistics  joinTime = new SummaryStatistics();
		SummaryStatistics  rankTime = new SummaryStatistics();
		SummaryStatistics  lastJoinbuckets = new SummaryStatistics();
		SummaryStatistics  estSources = new SummaryStatistics();
		SummaryStatistics  realStmt= new SummaryStatistics();
		SummaryStatistics  qtreeAllStmts= new SummaryStatistics();
		SummaryStatistics  top10= new SummaryStatistics();
		SummaryStatistics  top50= new SummaryStatistics();
		SummaryStatistics  top100= new SummaryStatistics();
		SummaryStatistics  top200= new SummaryStatistics();

		Double top10V=null, top50V=null,top100V=null,top200V=null;
		//	int top10=0,top50=0,top100=0,top200=0;
		int ntop10=0,ntop50=0,ntop100=0,ntop200=0;
		s.nextLine();
		while(s.hasNextLine()){
			String [] tokens = s.nextLine().split(" ");

			if(Double.valueOf(tokens[6])==0) continue;
			totalTime.addValue(Double.valueOf(tokens[1]));
			joinTime.addValue(Double.valueOf(tokens[2]));
			rankTime.addValue(Double.valueOf(tokens[3]));

			lastJoinbuckets.addValue(Double.valueOf(tokens[4]));
			estSources.addValue(Double.valueOf(tokens[5]));

			realStmt.addValue(Double.valueOf(tokens[6]));
			qtreeAllStmts.addValue(Double.valueOf(tokens[7])/Double.valueOf(tokens[6]));

			top10.addValue(Double.valueOf(tokens[8])/Double.valueOf(tokens[6]));
			top50.addValue(Double.valueOf(tokens[9])/Double.valueOf(tokens[6]));
			top100.addValue(Double.valueOf(tokens[10])/Double.valueOf(tokens[6]));
			top200.addValue(Double.valueOf(tokens[11])/Double.valueOf(tokens[6]));



		}
		s.close();
		
		File out = new File(logfile.getParentFile(),logfile.getName().replace(".dat", ".stats"));
		FileOutputStream fos = new FileOutputStream(out);
		write(fos,"totalTime",totalTime);
			write(fos,"joinTime",joinTime);
			write(fos,"rankTime",rankTime);

			write(fos,"buckets",lastJoinbuckets);
			write(fos,"sources",estSources);

			write(fos,"realStmt",realStmt);
			write(fos,"qtreeStmt",qtreeAllStmts);

			write(fos,"top10",top10);
			write(fos,"top50",top50);
			write(fos,"top100",top100);
			write(fos,"top200",top200);
		fos.close();
	}

	private void write(FileOutputStream fos, String string,
			SummaryStatistics totalTime) throws IOException {
fos.write((string+" "+totalTime.getN()+" "+totalTime.getMin()+" "+totalTime.getMean()+" "+totalTime.getMax()+" "+totalTime.getStandardDeviation()+" "+totalTime.getGeometricMean()+"\n").getBytes());
		
	}
}
