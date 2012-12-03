package ie.deri.urq.realidy.bench.utils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class CreateBenchmarkConfig {



	public static void main(String[] args) {

		File outDir = new File("/webstar1/datasum/eval-big");
		File input = new File("/home/jueumb/semqtree/input/data-all-04-clean.0.5.nq.gz");
		File queryFile = new File("/home/jueumb/semqtree/evaluation/web_data/random_queries/");

		File out = new File("evaluation/web_data/configs/");
		String prefix="eval-big";
		String [] versions = {"qtree","histo"};
		String [] hashings = {"adv_ns","mixed","adv_int2"};
		String [] queries = {"p-50-0","p-50-1","p-50-2","s-50-2","s-50-1","s-50-0"};
		for(String hashing: hashings){
			for(String version: versions){
				for(String query: queries){
					File configFile = new File(out,prefix+"_"+version+"-"+hashing+"-"+query+".config");
					FileWriter fw;
					try {
						fw = new FileWriter(configFile);
						StringBuilder sb = new StringBuilder();
						sb.append("out_dir=").append(outDir.getAbsolutePath()).append("\n");
						sb.append("index_version=").append(version).append("\n");
						sb.append("max_dim=").append("1000000").append("\n");

						sb.append("buckets=").append("50000").append("\n");
						sb.append("fanout=").append("20").append("\n");
						sb.append("storedetailedCount=").append(false).append("\n");
						sb.append("hashing_function=").append(hashing).append("\n");
						sb.append("input_data=").append(input.getAbsolutePath()).append("\n");

						sb.append("hashing=").append(true).append("\n");

						sb.append("insert=").append(true).append("\n");

						sb.append("query=").append(true).append("\n");
						sb.append("queries=").append(new File(queryFile,"eval-"+query).getAbsolutePath()).append("\n");
						sb.append("rerun=").append(false).append("\n");
						fw.write(sb.toString());
						fw.flush();
						fw.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}}}
		File configFile = new File(out,"run-all-"+prefix+".sh");
		FileWriter fw;
		try {
			fw = new FileWriter(configFile);
			StringBuilder sb = new StringBuilder();
			sb.append("#!/bin/bash\n");

			sb.append("BENCH_DIR=\n");
			sb.append("mkdir $BENCH_DIR");
			sb.append("DATASUM_ROOT=\n");
			sb.append("CONFIG_FILES=$DATASUM_ROOT/evaluation/web_data/configs\n");			
			sb.append("JAR=$DATASUM_ROOT/dist/realidy-dev.jar\n");
			sb.append("PREFIX=").append(prefix).append("\n"); 

			sb.append("for i in 0 1 2 3 \ndo\n");
			sb.append("  for index in qtree histo\n  do\n");
			sb.append("    for hash in prefix mixed bphash fnvhash\n    do\n");
			sb.append("      for config in `ls -1 $CONFIG_FILES/$PREFIX.$index*$hash*$i.config`\n      do\n");
			sb.append("        function my_basename {\n");
			sb.append("        typeset v x\n");
			sb.append("        v=${1##*/}\n");
			sb.append("        x=${2#.} # get rid of the '.'\n");
			sb.append("        v=${v%.$x}\n");
			sb.append("        echo $v\n");
			sb.append("        }\n");
			sb.append("        bn=$(my_basename $config)\n");
			sb.append("        echo \"Running config file $config\"\n");
			sb.append("        java -Xmx4G -jar $JAR BenchmarkSuite -config $config 2> $BENCH_DIR/$bn.stderr\n"); 
			sb.append("      done\n");
			sb.append("    done\n");
			sb.append("  done\n");
			sb.append("done\n");
			fw.write(sb.toString());
			fw.close();			
		}catch(Exception e){e.printStackTrace();}

	}
}
