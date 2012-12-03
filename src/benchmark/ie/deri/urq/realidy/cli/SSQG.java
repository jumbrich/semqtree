package ie.deri.urq.realidy.cli;



import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.Variable;
import org.semanticweb.yars2.cli.CreateSparse;
import org.semanticweb.yars2.index.NodeArrayIterator;
import org.semanticweb.yars2.index.disk.QuadStringIndex;
import org.semanticweb.yars2.index.disk.QuadStringScanIterator;
import org.semanticweb.yars2.index.disk.block.NodeBlockInputStream;
import org.semanticweb.yars2.index.disk.lookup.SparseIndex;

public class SSQG extends CLIObject{
    
	    
    private static boolean walk(Node subj, QuadStringIndex idx,
	    int leave, List<Nodes> nodes) throws IOException {
	System.err.println(" walk starting at "+subj+" for leaves "+leave);
	
	Set<Nodes> objects = new HashSet<Nodes>();
	try{
	NodeArrayIterator iter = idx.getQuadStringIterator(new Node[]{subj},new Variable[]{});
	Node[] walks;
	while(iter.hasNext()){
	    walks = iter.next();
	   if(walks != null)
	       objects.add(new Nodes(Arrays.copyOfRange(walks, 0, 3)));
	    
	}
	System.err.println("  possible walks = "+objects.size());
	Nodes obj;
	do{
	    if(objects.size()==0) return false;
		
	    int randomPos = (int) (Math.random() * nodes.size());
	    obj = drawRandom(objects, randomPos);
	    if(obj!=null){
		nodes.add(obj);
	    	System.err.println("  drawing walk "+obj+" ("+randomPos+") from "+objects.size()+" possibilities");
	    }
	    objects.remove(obj);
	}while(nodes.size()!=leave);
	
	System.out.println("Found obj "+obj);
//	nodes.add(obj);
	
		return true;
	}catch(Exception e){
	   System.err.println("[ERRPR] "+e.getMessage());
	    return false;
	}
    }

    private static Nodes drawRandom(Set<Nodes> objects, int randomPos) {
	int counter = 0;
	for(Nodes cur: objects){
	    if(counter == randomPos)
		return cur;
	    counter++;
	}
	return null;
    }


    private static Node drawRandom(Set<Node> allSubjects, int randomSubjPos) {
	int counter = 0;
	for(Node cur: allSubjects){
	    if(counter == randomSubjPos)
		return cur;
	    counter++;
	}
	return null;
    }

    private static Set<Node> getAllSubjects(File idx) {
	System.err.println("Getting all subjects");
	HashSet<Node> set = new HashSet<Node>();
	try {
	    NodeBlockInputStream nbis = new NodeBlockInputStream(idx.getAbsolutePath());
	    QuadStringScanIterator iter = new QuadStringScanIterator(nbis);
	    while(iter.hasNext()){
	    	set.add(iter.next()[0]);
	    }
	    
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	
	return set;
	
	
    }



    @Override
    public String getDescription() {
	return "create star-shaped queries";
    }



	@Override
	protected void addOptions(Options opts) {
		opts.addOption(CLIObject.OPTION_SAMPLING_SIZE);
		opts.addOption(CLIObject.OPTION_GENERIC_INPUT);
		opts.addOption(CLIObject.OPTION_GENERIC_OUTPUT);
		opts.addOption(CLIObject.OPTION_NUMBER_OF_JOINS);
		
	}



	@Override
	protected void execute(CommandLine cmd) {
		try{
			
		File idxFile = new File(cmd.getOptionValue(CLIObject.PARAM_GENERIC_INPUT));
		
		Set<Node> allSubjects = getAllSubjects(idxFile);

		
		NodeBlockInputStream nbis = new NodeBlockInputStream(idxFile.getAbsolutePath());
		SparseIndex spi = CreateSparse.createSparseIndex(nbis);
		QuadStringIndex idx = new QuadStringIndex(nbis, spi);

		int sizeValue = Integer.parseInt(cmd.getOptionValue(CLIObject.PARAM_SAMPLING_SIZE));
		int depthValue = Integer.parseInt(cmd.getOptionValue(CLIObject.PARAM_NUMBER_OF_JOINS));

		System.err.println("Walking");
		Set<List<Nodes>> results = new HashSet<List<Nodes>>();
		for(int i =0; i < sizeValue; i++){
		    System.err.println(">Walk-"+i);
		    List<Nodes> result;
		    boolean success;
		    do{
			result = new ArrayList<Nodes>();
			if(allSubjects.size()==0){
			    break;
			}
			int randomSubjPos = (int) (Math.random()*allSubjects.size());
			Node subj = drawRandom(allSubjects,randomSubjPos);
			System.err.println("[DRAWING] subj "+subj+" "+randomSubjPos+" from "+allSubjects.size()+" possibilities");
			allSubjects.remove(subj);
			
			walk(subj,idx,depthValue, result);
		 
			System.out.println("[STOPPED] walk contains "+result.size());
		    }while(result.size()!=depthValue);
		    System.err.println("[FOUND] result = "+result.size());
		    if(result.size()==depthValue) results.add(result);
		}
		
		File outdir = new File(cmd.getOptionValue("o"));
		outdir.mkdirs();
		
		System.err.println("Result contains (in reverse order");
		int counter =0;
		for(List<Nodes> nodes : results){
			counter++;
			File queryFile = new File(outdir,"star."+counter+".sparql");
			PrintWriter pw = new PrintWriter(queryFile);
			StringBuilder sb = new StringBuilder();
			Variable s = new Variable("join");
			String sel = "SELECT "+s.toN3();
			ArrayList<Nodes>q = new ArrayList<Nodes>();
			for(int i = nodes.size()-1; i>-1; i--){
				Nodes stmt = nodes.get(i);
				Node[] n =stmt.getNodes();
				q.add(new Nodes(s,n[1],n[2]));
//				sb.append(new Nodes(s,n[1],n[2])+"\n");
			}
		    for(int i =q.size()-1; i > -1; i --){
				sb.append(" ").append(q.get(i).toN3()).append("\n");
			}
			pw.println(sel+"\nWHERE{\n"+sb.toString()+"}\n");
			pw.flush();
			pw.close();
		}
		
		}catch(Exception e){
			e.printStackTrace();
		}
	}
    
    
}
