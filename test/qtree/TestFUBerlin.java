package qtree;
import ie.deri.urq.wods.hashing.CheatingSimpleHashing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Vector;

import junit.framework.TestCase;


import de.ilmenau.datasum.index.qtree.QTree;
import de.ilmenau.datasum.index.qtree.QTreeBucket;
import de.ilmenau.datasum.util.StringHelper;

/**
 * 
 */

/**
 * @author hose
 *
 */
public class TestFUBerlin extends TestCase{
	
	
	/**
	 * reads input data from a file
	 * @param directory 
	 * @param filename
	 * @return
	 */
	public static Vector<String[]> readInputFile_FUBerlin(String directory, String filename, String delimiter, int nmbOfDimensions){
		
		// collecting data arrays, each having <emph>nmbOfDimensions</emph> dimensions
		Vector<String[]> objects = new Vector<String[]>();
		
		try {
			// initialize reader
			BufferedReader reader = new BufferedReader(new FileReader(new File(directory+filename)));
			// read first line
			String line = reader.readLine();
			
			// while there are more lines to read
			while (line != null){
				//System.out.println(line);
				// split the input line using the given delimiter, e.g., " "
				String[] split = line.split(delimiter);
				//System.out.println(Arrays.toString(split));
				
				// if the number of pieces does not match the number of dimensions, there is a dimension containing delimiter characters using " to denote beginning and end of the dimensional string
				if (split.length != nmbOfDimensions && line.contains("\"")){
					// initialize new Array
					String[] newSplit = new String[nmbOfDimensions];
					int newSplitPos = 0;
					
					// run through all split strings and merge them if surrounded by "
					boolean openingFound = false;
					for (int i=0;i<split.length;i++){
						//System.out.println(Arrays.toString(newSplit));
						if (!split[i].contains("\"")){
							if (openingFound){
								// strings between "
								newSplit[newSplitPos-1] = newSplit[newSplitPos-1] + delimiter + split[i];
								//System.out.println(split[i]);
							} else {
								// string without delimiter
								newSplit[newSplitPos] = split[i];
								newSplitPos++;
							}
							
						} else {
							if (!openingFound){
								// first part of the string with delimiters
								openingFound = true;
								newSplit[newSplitPos] = split[i].substring(1);
								newSplitPos++;
							} else {
								// last part of the string with delimiters
								openingFound = false;
								newSplit[newSplitPos-1] = newSplit[newSplitPos-1] + delimiter + split[i].substring(0, split[i].length() - 1);
							}
						}
						
						// end if <emph>nmbOfDimensions</emph> Strings have been found
						if (newSplitPos ==nmbOfDimensions && !openingFound){
							break;
						}
						
					}
					//System.out.println(Arrays.toString(newSplit));
					objects.add(newSplit);
					
				} else {
					//System.out.println(Arrays.toString(split));
					
					String[] newSplit = new String[nmbOfDimensions];
					for (int i=0;i<nmbOfDimensions;i++){
						newSplit[i] = split[i];
					}
					//System.out.println(Arrays.toString(newSplit));
					
					objects.add(newSplit);
				}
				
				line = reader.readLine();
			}
			reader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

		return objects;
	}
	
	
	/**
	 * 
	 * @param fileName
	 * @param pathToDiagramDirectory
	 * @return
	 * @throws IOException
	 */
	public static BufferedWriter createNewResultFile(String fileName, String pathToDiagramDirectory) throws IOException {
		// false means: do not append to an existing file --> overwrite it
		return new BufferedWriter(new FileWriter(new File(
			pathToDiagramDirectory + fileName
		), false));
	}
	
	
	/**
	 * 
	 * @param q
	 * @param outputfile
	 * @param data
	 * @param printTree 
	 * @param printData 
	 */
	public static void printQTreeAsGNUPlot(QTree q, 
			                               String outputfile,
			                               String outputFolder,
			                               int [][] data, boolean printTree, boolean printData, 
			                               int dim1, int dim2){
		try {
			System.out.println("printing data");
			BufferedWriter writer = createNewResultFile(outputfile + ".gnuplot", outputFolder);
			writer.write("set terminal postscript eps 20 color \n");
			writer.write("set output \"" + outputfile + ".ps\"\n");
			writer.write("set size square\n");
			writer.write("set nokey\n");
			if (!(printTree) && !(printData)) return;
			writer.write("plot ");
			
			writer.write("[-50:1050] [-50:1050] ");
			if (printData) writer.write("'-' t \"DataSet\" with points lt 3 lw 10 pt 0, ");
			if (printTree) writer.write("'-' " + "t \"" + "Buckets" + "\" " + "with lines" 
		              + " lt " + "1" + " lw "+ "3"+ "\n"); 
		    else {
		    	writer.write("\n");
		    };
			/*if (printData){
				for (int i=0;i<data.length;i++){
					if (data[i].length == 2){
						writer.write(data[i][0]+" "+data[i][1]+"\n");
					}
					if (data[i].length == 3){
						writer.write(data[i][0]+" "+data[i][1]+" "+data[i][2]+"\n");
					}
				}
				writer.write("e"+"\n");
			}*/
			if (printData){
				for (int i=0;i<data.length;i++){
					/*if (data[i].length == 2){
						writer.write(data[i][0]+" "+data[i][1]+"\n");
					}
					if (data[i].length == 3){
						writer.write(data[i][0]+" "+data[i][1]+" "+data[i][2]+"\n");
					}*/
					writer.write(data[i][dim1]+" "+data[i][dim2]+"\n");
				}
				writer.write("e"+"\n");
			}
			if (printTree){
				//int xMinStep = 1;
				//int yMinStep = 1;
				Vector <QTreeBucket> buckList = q.getAllBuckets();
				for (QTreeBucket qtb : buckList){
					int [] min = qtb.getLowerBoundaries();
					int [] max = qtb.getUpperBoundaries();
					// We get cloned Arays back, so we could modify without changes	
					/*
				// X-Achse				
				if (max[0] == min[0]) {
					max[0] += xMinStep;
					min[0] -= xMinStep;
				}
				// Y-Achse
				if (max[1] == min [1]) {
					max[1] += yMinStep;
					min[1] -= yMinStep;					

				}
					 */
					/*if (min[dim1] == min[dim2]){
						min[dim1] = min[dim1]-1;
					}
					if (max[dim1] == min[dim2]){
						min[dim2] = min[dim2]+1;
					}*/
					if (min[dim1] == min[dim2] && max[dim1] == max[dim2]){
						min[dim1] = min[dim1] - 1;
						max[dim2] = max[dim2] + 1;
					}
					writer.write(min[dim1]+" "+min[dim2]+"\n");
					writer.write(max[dim1]+" "+min[dim2]+"\n\n");

					writer.write(max[dim1]+" "+min[dim2]+"\n");
					writer.write(max[dim1]+" "+max[dim2]+"\n\n");

					writer.write(max[dim1]+" "+max[dim2]+"\n");
					writer.write(min[dim1]+" "+max[dim2]+"\n\n");

					writer.write(min[dim1]+" "+max[dim2]+"\n");
					writer.write(min[dim1]+" "+min[dim2]+"\n\n");
				}			
			}
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	
	public void testFUBerlin() throws Exception {
		int dimensions = 3;
		int fanout = 5;
		int buckets = 30;
		int[] dimSpecMin = {0,0,0};
		int[] dimSpecMax = {1000,1000,1000};
		
		boolean storeDetailedCounts = false;
		
		int minCharValue = 32;
		int maxCharValue = 126;
		
		boolean debug = false;
		
		// read data
		//Vector<String[]> stringData = readInputFile("input/", "test.txt", " ", dimensions);
		Vector<String[]> stringData = readInputFile_FUBerlin("input/", "fu-berlin.de.nq", " ", dimensions);
		
		// remove longest common prefixes and suffixes
		StringHelper.trimmingCommonPrefixesAndSuffixes(stringData);
		
		// collect int coordinate data
		int[][] data = new int[stringData.size()][dimensions];
		
		// initiate QTree
		QTree testQTree = new QTree(dimensions, fanout, buckets, dimSpecMin, dimSpecMax, "0", "1",storeDetailedCounts);
		
		
		// compute minimum and maximum hash values for all dimensions -- and set the parameters in the CheatingSimpleHashing class
		CheatingSimpleHashing cheatHasher = new CheatingSimpleHashing(minCharValue, maxCharValue); // all ASCII codes between these two 32 = whitespace, 126 = ~ 
		Vector<int[]> minMaxValues = cheatHasher.getMinMaxHashValuesForAllDimensions(stringData, dimSpecMin, dimSpecMax);
		int[] minValues = minMaxValues.firstElement();
		int[] maxValues = minMaxValues.elementAt(1);
		
		// or in short, these are the precomputed ones for the FU-Berlin test set
		/*int[] MIN_VALUES = {110,157,104};
		int[] MAX_VALUES = {410,422,379};*/
		
		
		// insert data into QTree
		for (int i=0;i<stringData.size();i++){
			
			String[] item = stringData.elementAt(i);
			
			// get hash coordinates using the simple hasher
			/*SimpleHashing simpleHasher = new SimpleHashing();
			int[] hashCoordinates = simpleHasher.getHashCoordinates(item, dimSpecMin, dimSpecMax);*/
			
			// get hash coordinates using the cheating simple hasher
			CheatingSimpleHashing cheatingHasher = new CheatingSimpleHashing(minValues, maxValues, minCharValue, maxCharValue); // all ASCII codes between these two 32 = whitespace, 126 = ~
			int[] hashCoordinates = cheatingHasher.getRescaledHashCoordinates(item, dimSpecMin, dimSpecMax);
			
			data[i] = hashCoordinates;
			if (debug) System.out.println(Arrays.toString(hashCoordinates));
			
			// insert into QTree
			try {
				testQTree.insertDataItem(hashCoordinates, "fu-berlin.de.nq");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		/*int[] item = {800,10,500};
		//data[0] = item;
		int[] item2 = {100,100,100};
		//data[1] = item2;
		int[] item3 = {500,500,500};
		//data[2] = item3;
		try {
			testQTree.insertDataItem(item);
			testQTree.insertDataItem(item2);
			testQTree.insertDataItem(item3);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
		// output current QTree state as a gnuplot diagram
		printQTreeAsGNUPlot(testQTree, "data", "output/", data, true, true, 1, 2);
	}

}
