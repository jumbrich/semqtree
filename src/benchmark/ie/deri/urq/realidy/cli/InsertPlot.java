package ie.deri.urq.realidy.cli;

import ie.deri.urq.realidy.bench.utils.Printer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Scanner;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InsertPlot extends CLIObject{
	private final static Logger log = LoggerFactory.getLogger(InsertPlot.class);



	public static void main(String[] args) {
		InsertPlot e = new InsertPlot();
		e.run(new String[]{"-i","/Users/juum/Tmp/evalDumps/ds3/insert", "-o","/Users/juum/Tmp/evalDumps/ds3/plots"});
	}

	@Override
	protected void addOptions(Options opts) {
		//each benchmark needs an output file
		opts.addOption(CLIObject.OPTION_GENERIC_OUTPUT);
		opts.addOption(CLIObject.OPTION_GENERIC_INPUT);
	}
	@Override
	protected void execute(CommandLine cmd) {
		try {
			File outDir = new File(cmd.getOptionValue(CLIObject.PARAM_GENERIC_OUTPUT));
			if(!outDir.exists()) outDir.mkdirs();
			File inDir = new File(cmd.getOptionValue(CLIObject.PARAM_GENERIC_INPUT));

			FileOutputStream fis = new FileOutputStream(new File(outDir,"insert_plot-all.gnuplot"));

			File [] files = inDir.listFiles(new FilenameFilter() {
				public boolean accept(File dir, String name) {
					return name.contains("stmts");
				}
			});
			StringBuilder sb = new StringBuilder(Printer.readFile("insert-plot.gnuplot.tmpl"));
			
			sb.append("\nplot");
			
			int counter =1;
			for(File f: files){
				String prefix = f.getName().substring(0,f.getName().indexOf("_"));
				sb.append("'").append(f.getAbsolutePath()).append("' u 1:2 with lp t 'total [").append(prefix).append("]'  pt 1 lt ").append(counter).append(",\\\n");
				sb.append("'").append(f.getAbsolutePath()).append("' u 1:3 with lp t 'diff [").append(prefix).append("]'  pt 1 lt ").append(counter);
				if(counter < files.length/2){
					sb.append(",\\\n");
				}
				counter++;
			}
			fis.write(sb.toString().getBytes());
			fis.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private SummaryStatistics parse(File f,FileOutputStream fis) throws IOException {
		Scanner s = new Scanner(f);
		SummaryStatistics avgStmtsPerMS = new SummaryStatistics();
		SummaryStatistics avgMSPerStmt = new SummaryStatistics();
		while(s.hasNextLine()){
			String line = s.nextLine().trim();
			String [] tokens = line.split(" ");
			long time = Long.valueOf(tokens[1]);
			long diff = Long.valueOf(tokens[2]);
			int stmts = Integer.valueOf(tokens[0]);
			log.info("stmts:{} total:{} {} diff:{} {} ", new Object[]{tokens[0],tokens[1],(double) stmts/(double)time,tokens[2],(double) 10000/(double)diff});

			avgMSPerStmt.addValue((double) diff/(double)10000);
			avgStmtsPerMS.addValue((double) 10000/(double)diff);
		}
		s.close();

		//		log.info("avg. stmts/ms: {}",avgInsertTime.getMean());
		log.info("avg. diff stmts/ms: {}",avgStmtsPerMS.getMean());
		fis.write((f.getName().substring(0,f.getName().indexOf("_"))+","+avgStmtsPerMS.getMean()+","+avgMSPerStmt.getMean()+"\n").getBytes());
		fis.flush();
		return avgStmtsPerMS;
	}

	@Override
	public String getDescription() {
		return "benchmark SPARQL queries";
	}
	
}
