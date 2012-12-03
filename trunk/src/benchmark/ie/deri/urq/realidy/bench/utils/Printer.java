package ie.deri.urq.realidy.bench.utils;

import ie.deri.urq.realidy.cli.InsertPlot;
import ie.deri.urq.realidy.index.SemQTree;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.TreeSet;
import java.util.Vector;

import org.semanticweb.yars.stats.Count;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.ilmenau.datasum.index.AbstractIndex;
import de.ilmenau.datasum.index.SingleQTreeIndex;
import de.ilmenau.datasum.index.qtree.QTreeBucket;

public class Printer {


	private static final Logger log = LoggerFactory.getLogger(Printer.class);

//	private static final String graphicExtenstion = ".eps";
	private static final String graphicExtenstion = ".png";
//	private static final String setTerminal = "set term postscript eps enhanced";
	private static final String setTerminal = "set term png";
	
	
	public static void printBuckets(SemQTree idx, File dir, File plotRoot) throws IOException {
		if(!dir.exists())dir.mkdirs();
		AbstractIndex aIdx = idx.getAbstractIndex();
		Vector <QTreeBucket> buckList = null;
		if(aIdx instanceof SingleQTreeIndex){
			buckList = ((SingleQTreeIndex) aIdx)._sourcesQTree.getAllBuckets();
		}

		
		printBuckets(buckList,dir,plotRoot,idx.getAbstractIndex().getHasher());
		
		
	}

	private static void printBuckets(Vector<QTreeBucket> buckList, File dir, File plotRoot, String hash) throws IOException {
		FileWriter fwSP= new FileWriter(new File(dir,"buckets-"+hash+".1.2.dat"));
		FileWriter fwSO= new FileWriter(new File(dir,"buckets-"+hash+".1.3.dat"));
		FileWriter fwPO= new FileWriter(new File(dir,"buckets-"+hash+".2.3.dat"));
		int s =0, p=1 , o=2;
		for (QTreeBucket qtb : buckList){
			
			int [] min = qtb.getLowerBoundaries();
			int [] max = qtb.getUpperBoundaries();
		
			printDim(fwSP,s,p,min,max);
			printDim(fwPO,o,p,min,max);
			printDim(fwSO,o,s,min,max);
			
		}
		fwSP.close();
		fwSO.close();
		fwPO.close();
		
		printGnuplotBuckets(3,1,hash,plotRoot,dir);
		printGnuplotBuckets(3,2,hash,plotRoot,dir);
		printGnuplotBuckets(1,2,hash,plotRoot,dir);
		
		
		
	}

	private static void printGnuplotBuckets(int i, int j ,String hash, File plotRoot,File data) throws IOException {
		FileWriter writer = new FileWriter(new File(plotRoot,"buckets-"+hash+"_"+i+"_"+j+".gnuplot"));
//		writer.write("set terminal postscript eps 20 color \n");
		writer.write(setTerminal+" \n");
		writer.write("set output \"" + new File(plotRoot,"buckets-"+hash+"_"+i+"_"+j+""+graphicExtenstion).getName()+"\"\n");
		writer.write("set size square\n");
		writer.write("set nokey\n");
		writer.write("plot ");
		
//		writer.write("[-50:"+(max+50)+"] [-50:"+(max+50)+"] ");
//		if (printData) writer.write("'-' t \"DataSet\" with points lt 3 lw 10 pt 0, ");
		File input= new File(data,"buckets-"+hash+"."+i+"."+j+".dat");
		writer.write("'"+findRelativePath(plotRoot, input.getParentFile())+input.getName()+"' " + "t \"" + "Buckets" + "\" " + "with lines" 
	              + " lt " + "1" + " lw "+ "3"+ "\n"); 
	    writer.close();
		
	}

	private static void printDim(FileWriter writer, int dim1, int dim2, int[] min, int[] max) throws IOException {
		
		if (min[dim1] == min[dim2] && max[dim1] == max[dim2]){
			min[dim1] = min[dim1] - 1;
			max[dim2] = max[dim2] + 1;
//			System.out.println("We are equals for "+dim1+" "+dim2);
		}
		if (min[dim1] == max[dim1]){max[dim1] +=1;}
		if (min[dim2] == max[dim2]){max[dim2] +=1;}
		
		StringBuilder sb = new StringBuilder();
		sb.append(min[dim1]).append(" ").append(min[dim2]).append("\n");
		sb.append(max[dim1]).append(" ").append(min[dim2]).append("\n\n");
		

		sb.append(max[dim1]).append(" ").append(min[dim2]).append("\n");
		sb.append(max[dim1]).append(" ").append(max[dim2]).append("\n\n");
		
		sb.append(max[dim1]).append(" ").append(max[dim2]).append("\n");
		sb.append(min[dim1]).append(" ").append(max[dim2]).append("\n\n");
		
		sb.append(min[dim1]).append(" ").append(max[dim2]).append("\n");
		sb.append(min[dim1]).append(" ").append(min[dim2]).append("\n\n");
		writer.write(sb.toString());
		writer.flush();
//		System.out.println(sb.toString());
	}

	public static void produceOverlapPlot(String hasherName, File bucketsRoot,
			File hashRoot, File plotRoot) throws IOException {
			plotOverlap(1,2,hasherName,bucketsRoot,hashRoot,plotRoot);
		plotOverlap(1,3,hasherName,bucketsRoot,hashRoot,plotRoot);
		plotOverlap(2,3,hasherName,bucketsRoot,hashRoot,plotRoot);
		
		
	}

	private static void plotOverlap(int i, int j, String hash,
			File bucketsRoot, File hashRoot, File plotRoot) throws IOException {
		File out = new File(plotRoot,"overlap-"+hash+"_"+i+"_"+j+".gnuplot");
		FileWriter writer = new FileWriter(out);
//		writer.write("set terminal postscript eps 20 color \n");
		writer.write(setTerminal+" \n");
		writer.write("set output \"" + out.getName().replaceAll(".gnuplot",""+graphicExtenstion)+"\"\n");
		//writer.write("set size square\n");
		//writer.write("set nokey\n");
		writer.write("plot ");
		File input = new File(bucketsRoot,"buckets-"+hash+"."+i+"."+j+".dat");
		File dataFile = new File(hashRoot,"datahash-"+hash+".points");
		
		writer.write(" \""+dataFile.getAbsolutePath()+"\" u "+i+":"+j+" with p ps 0.2 t \"points\",");
		writer.write("'"+input.getAbsolutePath()+"' " + "t \"Buckets\" " + "with lines" 
	              + " lt " + "1" + " lw "+ "2"+ "\n"); 
		
		writer.close();
	}

	public static void gnuplot2DimHashPoints(File outDir, String hash, int i, int j, File plotDir, int maxDim) throws IOException {
		
		File out = new File(plotDir, "hashpoints-"+hash+"_"+i+"_"+j+".gnuplot");
		log.info("PRINT GNUPLOT FILE for {},{} to {}",new Object[]{i,j,out.getAbsolutePath()});
		FileWriter fw = new FileWriter(out);
//		fw.write("set terminal postscript eps 20\n");
		fw.write("set terminal png\n");
			fw.write("set output \""+out.getName().replaceAll(".gnuplot",".png")+"\"\n");
			fw.write("set xlabel \""+i+"\"\n");
			fw.write("set ylabel \""+j+"\"\n");
			fw.write("set pointsize 2\n");
			fw.write("set xrange [0:"+maxDim+"]\n");
			fw.write("set yrange [0:"+maxDim+"]\n");
//			if(yLogscale)
//				fw.write("set logscale y\n");
			File dataFile = new File(outDir,"datahash-"+hash+".points");
			fw.write("plot \""+findRelativePath(plotDir, dataFile.getParentFile())+dataFile.getName()+"\" u "+i+":"+j+"  with p ps 0.2 t \"\"\n");
			fw.flush();fw.close();
		}

	public static void printHashDist(Count<Integer> hashToS, int i, String hash, File out) throws IOException {
			log.info("PRINT HASH DIST for {} with {} entries to {}",new Object[]{i,hashToS.size(),out.getAbsolutePath()});
			
			File file = new File(out,"hashdist-"+hash+"."+i+".dat");
			FileWriter fw = new FileWriter(file);
			TreeSet<Entry<Integer,Integer>> ordered= new TreeSet<Entry<Integer,Integer>>(new Comparator<Entry<Integer,Integer>>() {

				public int compare(Entry<Integer, Integer> o1,
						Entry<Integer, Integer> o2) {
					return o1.getKey().compareTo(o2.getKey());
				}
			});
			ordered.addAll(hashToS.entrySet());
			for(Entry<Integer, Integer> dist: ordered){
				fw.write(dist.getKey()+" "+dist.getValue()+"\n");
				fw.flush();
			}
			fw.close();
	}

	public static void printHashDistPlot(int i,
			File out, String hash, File plotDir) throws IOException {
		
		File plotout = new File(plotDir,"hashdist-"+hash+"_"+i+".gnuplot");
		FileWriter fw = new FileWriter(plotout);
//		fw.write("set terminal postscript eps 20\n");
		fw.write("set term postscript eps enhanced\n");
		fw.write("set output \""+plotout.getName().replaceAll(".gnuplot",".eps")+"\"\n");
			fw.write("set xlabel \""+i+"\"\n");
			fw.write("set ylabel \"frequency\"\n");
			fw.write("set pointsize 2\n");

//			if(yLogscale)
//				fw.write("set logscale y\n");
			File dataFile = new File(out,"hashdist-"+hash+"_"+i+".dat");
			fw.write("plot \""+findRelativePath(plotDir, dataFile.getParentFile())+dataFile.getName()+"\" u 1:2  with p ps 0.2 t \""+i+"\"\n");
			fw.flush();fw.close();
	}

	
	public static void printQueryPlots(File outFile, File plotDir) throws IOException {
		File plotfile = new File(plotDir,outFile.getName().replace(".dat", ".gnuplot"));
		FileWriter fw = new FileWriter(plotfile);
//		fw.write("set terminal postscript eps 20\n");
		fw.write(setTerminal+" \n");
		fw.write("set output \""+plotfile.getName().replace(".gnuplot","_time"+graphicExtenstion)+"\"\n");
		fw.write("set xlabel \"queries\"\n");
		fw.write("set ylabel \"Time in milliseconds\"\n");
		fw.write("plot \""+findRelativePath(plotDir, outFile.getParentFile())+outFile.getName()+"\" u 1:2 with lp t \"total\", \"\" u 1:3 with lp t \"join\", \"\" u 1:4 with lp t \"rank\"\n\n");

		
		fw.write("set output \""+plotfile.getName().replace(".gnuplot","_perc"+graphicExtenstion)+"\"\n");
		fw.write("set xlabel \"queries\"\n");
		fw.write("set logscale y\n");
		fw.write("set ylabel \"fraction of results\"\n");
		fw.write("plot \""+findRelativePath(plotDir, outFile.getParentFile())+outFile.getName()+"\"  u 1:8 with lp t \"qtree\", \"\" u 1:9 with lp t \"top10\", \"\" u 1:10 with lp t \"top50\", \"\" u 1:11 with lp t \"top100\", \"\" u 1:12 with lp t \"top200\"\n");
		fw.close();
		
		
	}

	public static void printInsertPlot(File outTimeDAT, File outStmtDAT, File plotDir) throws IOException {
		File plotfile = new File(plotDir,outTimeDAT.getName().replace(".dat", ".gnuplot"));
		
		FileWriter fw = new FileWriter(plotfile);
		fw.write(setTerminal+" \n");
		fw.write("set output \""+plotfile.getName().replace(".gnuplot","_count"+graphicExtenstion)+"\"\n");
		
		fw.write("set xlabel \"time in ms\"\n");
		fw.write("set logscale y\n");
		fw.write("set ylabel \"count\"\n");
		//time count diff used free
		fw.write("plot \""+findRelativePath(plotDir, outTimeDAT.getParentFile())+outTimeDAT.getName()+"\" u 1:2 with l t \"total\", \"\" u 1:3 with l t \"diff\"\n\n");
		
		fw.write("set output \""+plotfile.getName().replace(".gnuplot","_mem"+graphicExtenstion)+"\"\n");
		fw.write("set xlabel \"time in ms\"\n");
		fw.write("set logscale y\n");
		fw.write("set ylabel \"memory in KBytes\"\n");
		fw.write("plot \""+findRelativePath(plotDir, outTimeDAT.getParentFile())+outTimeDAT.getName()+"\"  u 1:4 with l t \"used\", \"\" u 1:5 with l t \"free\"\n");
		fw.close();


		plotfile = new File(plotDir,outStmtDAT.getName().replace(".dat", ".gnuplot"));
		
		fw = new FileWriter(plotfile);
		fw.write(setTerminal+" \n");

		fw.write("set output \""+plotfile.getName().replace(".gnuplot","_time"+graphicExtenstion)+"\"\n");
		fw.write("set xlabel \"count\"\n");
		fw.write("set logscale y\n");
		fw.write("set ylabel \"time in ms\"\n");
		//count time diffTime used free
		fw.write("plot \""+findRelativePath(plotDir, outStmtDAT.getParentFile())+outStmtDAT.getName()+"\" u 1:2 with l t \"total\", \"\" u 1:3 with l t \"diff\"\n\n");
		
		fw.write("set output \""+plotfile.getName().replace(".gnuplot","_mem"+graphicExtenstion)+"\"\n");
		fw.write("set xlabel \"count\"\n");
		fw.write("set logscale y\n");
		fw.write("set ylabel \"memory in KBytes\"\n");
		fw.write("plot \""+findRelativePath(plotDir, outStmtDAT.getParentFile())+outStmtDAT.getName()+"\"  u 1:4 with l t \"used\", \"\" u 1:5 with l t \"free\"\n");
		fw.close();
		
	}
	public static String findRelativePath(File base, File path)
    throws IOException
 {
    String a = base.getCanonicalFile().toURI().getPath();
    String b = path.getCanonicalFile().toURI().getPath();
    String[] basePaths = a.split("/");
    String[] otherPaths = b.split("/");
    int n = 0;
    for(; n < basePaths.length && n < otherPaths.length; n ++)
    {
       if( basePaths[n].equals(otherPaths[n]) == false )
          break;
    }
    StringBuffer tmp = new StringBuffer("../");
    for(int m = n; m < basePaths.length - 1; m ++)
       tmp.append("../");
    for(int m = n; m < otherPaths.length; m ++)
    {
       tmp.append(otherPaths[m]);
       tmp.append("/");
    }

    return tmp.toString();
 }
	
	
	public static void main(String[] args) throws IOException {
		File base = new File("/a/b/c/d/e/f");
		File path = new File("/a/b/c/d/test.html");
		
		System.out.println(findRelativePath(base, path)+path.getName());
	}

	public static void printJoinOrderEval(File plotRoot, File dataFile, String title) throws IOException {
		File plotfile = new File(plotRoot,dataFile.getName().replace(".stats", ".gnuplot"));
		log.info("Plotting {}",plotfile.getAbsolutePath());
		double range = 0;
		Scanner s = new Scanner(dataFile);
		while(s.hasNextLine()) {
			s.nextLine();range++;
		}
		
		FileWriter fw = new FileWriter(plotfile);
		fw.write(setTerminal+" \n");
		fw.write("set output \""+plotfile.getName().replace(".gnuplot",graphicExtenstion)+"\"\n");
		fw.write("set xrange [-0.5:"+(range-0.5)+"]\n");
		fw.write("set xlabel \"\"\n");
		fw.write("set logscale y\n");
		fw.write("set xtic rotate by 45 scale 0\n");
		fw.write("set xtics out offset 0,-1.0\n");
		fw.write("set ylabel \""+title.replaceAll("_", "-")+" (logscale)\"\n");
		//time count diff used free
		
		fw.write("plot \""+dataFile.getName()+"\" " +
				"using 1:5:4:6 with errorbars t \"with join ordering\" lt 1," +
				"\"\"  using 1:5:xticlabel(2) w l t \"\" lt 1,"+
				"\"\" u ($1+0.05):12 w l t \"\" lt 2, \"\" using ($1+0.05):12:11:13 with errorbars t \"no join ordering\" lt 2" +
				"\n");
		fw.flush();
		fw.close();
	}

	public static void printNXEval(File plotRoot, File dataFile, String title) throws IOException {
		File plotfile = new File(plotRoot,dataFile.getName().replace(".stats", ".gnuplot"));
		log.info("Plotting {}",plotfile.getAbsolutePath());
		double range = 0;
		Scanner s = new Scanner(dataFile);
		while(s.hasNextLine()) {s.nextLine();range++;}
		
		FileWriter fw = new FileWriter(plotfile);
		fw.write(setTerminal+" \n");
		fw.write("set output \""+plotfile.getName().replace(".gnuplot",graphicExtenstion)+"\"\n");
		fw.write("set xrange [-0.5:"+(range-0.5)+"]\n");
		fw.write("set xlabel \"\"\n");
		fw.write("set logscale y\n");
		fw.write("set xtic rotate by -45 scale 0\n");
		fw.write("set xtics out offset 0,-2.0\n");
		fw.write("set ylabel \""+title.replaceAll("_", "-")+" (logscale)\"\n");
		//time count diff used free
		
		fw.write("plot \""+dataFile.getName()+"\" " +
				"using 1:4:3:5 with errorbars t \"min, mean, max\" lt 1," +
				"\"\"  using 1:4:xticlabel(2) w l t \"\" lt 1\n");
		fw.flush();
		fw.close();
		
	}

	public static String readFile(String string) {
		URL url = InsertPlot.class.getResource("/ie/deri/urq/realidy/bench/utils/insert-all.gnuplot.tmpl");
		
		BufferedReader br;
		StringBuilder sb = new StringBuilder();
		try {
			br = new BufferedReader(new FileReader(new File(url.toURI())));
			
			String line = null;
		
			while((line = br.readLine())!=null){
				sb.append(line).append("\n");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return sb.toString();
	}

	
}