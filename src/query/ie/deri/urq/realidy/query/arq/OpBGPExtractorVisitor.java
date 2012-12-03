package ie.deri.urq.realidy.query.arq;

import java.io.Serializable;

import com.hp.hpl.jena.sparql.algebra.OpVisitor;
import com.hp.hpl.jena.sparql.algebra.op.OpAssign;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpConditional;
import com.hp.hpl.jena.sparql.algebra.op.OpDatasetNames;
import com.hp.hpl.jena.sparql.algebra.op.OpDiff;
import com.hp.hpl.jena.sparql.algebra.op.OpDisjunction;
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct;
import com.hp.hpl.jena.sparql.algebra.op.OpExt;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpGraph;
import com.hp.hpl.jena.sparql.algebra.op.OpGroupAgg;
import com.hp.hpl.jena.sparql.algebra.op.OpJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpLabel;
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpList;
import com.hp.hpl.jena.sparql.algebra.op.OpNull;
import com.hp.hpl.jena.sparql.algebra.op.OpOrder;
import com.hp.hpl.jena.sparql.algebra.op.OpPath;
import com.hp.hpl.jena.sparql.algebra.op.OpProcedure;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.algebra.op.OpPropFunc;
import com.hp.hpl.jena.sparql.algebra.op.OpQuadPattern;
import com.hp.hpl.jena.sparql.algebra.op.OpReduced;
import com.hp.hpl.jena.sparql.algebra.op.OpSequence;
import com.hp.hpl.jena.sparql.algebra.op.OpService;
import com.hp.hpl.jena.sparql.algebra.op.OpSlice;
import com.hp.hpl.jena.sparql.algebra.op.OpTable;
import com.hp.hpl.jena.sparql.algebra.op.OpTriple;
import com.hp.hpl.jena.sparql.algebra.op.OpUnion;
import com.hp.hpl.jena.sparql.core.BasicPattern;

public class OpBGPExtractorVisitor implements OpVisitor, Serializable{

    
    private OpBGP _opBGP = null;
    private BasicPattern _pattern;
    
    public OpBGP getOpBGP(){return _opBGP;}
    public BasicPattern getBasicPattern (){return _pattern;} 
    
    public void reset(){_opBGP=null;}
    public void visit(OpBGP arg0) {
    	_opBGP = arg0;
    	_pattern = arg0.getPattern();

    }

    public void visit(OpQuadPattern arg0) {;}

    public void visit(OpTriple arg0) {;}

    public void visit(OpPath arg0) {;}

    public void visit(OpTable arg0) {;}

    public void visit(OpNull arg0) {;}

    public void visit(OpProcedure arg0) {;}

    public void visit(OpPropFunc arg0) {;}

    public void visit(OpFilter arg0) {;}

    public void visit(OpGraph arg0) {;}

    public void visit(OpService arg0) {;}

    public void visit(OpDatasetNames arg0) {;}

    public void visit(OpLabel arg0) {;}

    public void visit(OpJoin arg0) {;}

    public void visit(OpLeftJoin arg0) {;}

    public void visit(OpDiff arg0) {;}

    public void visit(OpUnion arg0) {;}

    public void visit(OpConditional arg0) {;}

    public void visit(OpSequence arg0) {;}

    public void visit(OpDisjunction arg0) {;}

    public void visit(OpExt arg0) {;}

    public void visit(OpList arg0) {;}

    public void visit(OpOrder arg0) {;}

    public void visit(OpProject arg0) {;}

    public void visit(OpReduced arg0) {;}

    public void visit(OpDistinct arg0) {;}

    public void visit(OpSlice arg0) {;}

    public void visit(OpAssign arg0) {;}

    public void visit(OpGroupAgg arg0) {;}
}
