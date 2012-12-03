package ie.deri.urq.realidy.bench.utils;

import ie.deri.urq.realidy.index.SemQTree;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Vector;

import de.ilmenau.datasum.index.AbstractIndex;
import de.ilmenau.datasum.index.SingleQTreeIndex;
import de.ilmenau.datasum.index.qtree.QTreeBucket;

public class PrintBuckets {


	public static void printBuckets(SemQTree idx, File dir) throws IOException {
		System.out.println(idx.info());
		if(!dir.exists())dir.mkdirs();
		AbstractIndex aIdx = idx.getAbstractIndex();
		Vector <QTreeBucket> buckList = null;
		if(aIdx instanceof SingleQTreeIndex){
			buckList = ((SingleQTreeIndex) aIdx)._sourcesQTree.getAllBuckets();
		}

		FileWriter fwSP= new FileWriter(new File(dir,"sp.buckets.dat"));
		FileWriter fwSO= new FileWriter(new File(dir,"so.buckets.dat"));
		FileWriter fwPO= new FileWriter(new File(dir,"po.buckets.dat"));
		int s =0, p=1 , o=2;
		for (QTreeBucket qtb : buckList){
			System.out.println(qtb);
			int [] min = qtb.getLowerBoundaries();
			int [] max = qtb.getUpperBoundaries();
		
			printDim(fwSP,s,p,min,max);
			printDim(fwSO,s,o,min,max);
			printDim(fwPO,p,o,min,max);
		}
		
		fwSP = new FileWriter(new File(dir,"sp.buckets.gnuplot"));
		fwSO = new FileWriter(new File(dir,"so.buckets.gnuplot"));
		fwPO = new FileWriter(new File(dir,"po.buckets.gnuplot"));
		printGnuplot(fwPO, new File(dir,"po.buckets"),new File(dir,"po.buckets.dat"),idx.getAbstractIndex().getDimSpecMax()[0]);
		printGnuplot(fwSP, new File(dir,"sp.buckets"),new File(dir,"sp.buckets.dat"),idx.getAbstractIndex().getDimSpecMax()[0]);
		printGnuplot(fwSO, new File(dir,"so.buckets"),new File(dir,"so.buckets.dat"),idx.getAbstractIndex().getDimSpecMax()[0]);
		
		
	}

	private static void printGnuplot(FileWriter writer, File outputfile, File input, int max) throws IOException {
		writer.write("set terminal postscript eps 20 color \n");
		writer.write("set output \"" + outputfile.getName() + ".ps\"\n");
		writer.write("set size square\n");
		writer.write("set nokey\n");
		writer.write("plot ");
		
		writer.write("[-50:"+(max+50)+"] [-50:"+(max+50)+"] ");
//		if (printData) writer.write("'-' t \"DataSet\" with points lt 3 lw 10 pt 0, ");
		writer.write("'"+input.getName()+"' " + "t \"" + "Buckets" + "\" " + "with lines" 
	              + " lt " + "1" + " lw "+ "3"+ "\n"); 
	    writer.close();
		
	}

	private static void printDim(FileWriter writer, int dim1, int dim2, int[] min, int[] max) throws IOException {
		
		if (min[dim1] == min[dim2] && max[dim1] == max[dim2]){
			min[dim1] = min[dim1] - 1;
			max[dim2] = max[dim2] + 1;
//			System.out.println("We are equals for "+dim1+" "+dim2);
		}
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
//		System.out.println(sb.toString());
	}
}