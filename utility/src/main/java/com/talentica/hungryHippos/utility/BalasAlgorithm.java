package com.talentica.hungryHippos.utility;

import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.optim.linear.LinearConstraintSet;
import org.apache.commons.math3.optim.linear.LinearObjectiveFunction;

/**
 * Created by debasishc on 12/8/15.
 */
public class BalasAlgorithm {
    public BalasAlgorithm(LinearConstraintSet constraints,
                          LinearObjectiveFunction objectiveFunction){

    }

    public IPProblem standardize(IPProblem problem){

        RealVector sourceCoeffs = problem.getObjectiveFunction().getCoefficients();
        double [] resultC = new double[sourceCoeffs.getDimension()];
        Transformation<Integer, Integer> [] targetTranformations = new Transformation[sourceCoeffs.getDimension()];

        Transformation<Integer, Integer> identityTransformation = (Integer x) -> x;
        Transformation<Integer, Integer> inverseTransformation = (Integer x) -> 1-x;
        double targetConstant = problem.getObjectiveFunction().getConstantTerm();

        for(int i=0;i<sourceCoeffs.getDimension();i++){
            double coeff = sourceCoeffs.getEntry(i);
            if(coeff>=0){
                resultC[i] = coeff;
                targetTranformations[i] = problem.getVarTransformationSet().transformations[i].compose(identityTransformation);
            }else{
                resultC[i] = -coeff;
                targetTranformations[i] = problem.getVarTransformationSet().transformations[i].compose(inverseTransformation);
                targetConstant+=coeff;


            }
        }
        return null;

    }

}
