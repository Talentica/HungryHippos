package com.talentica.hungryHippos.utility;


import java.util.Arrays;

/**
 * Created by debasishc on 12/8/15.
 */
public class IPProblem {
    private LinearObjectiveFunction objectiveFunction;
    private LinearObjectiveFunction originalObjectiveFunction;
    private LinearConstraint [] constraints;

    public IPProblem(LinearConstraint[] constraints,
                     LinearObjectiveFunction objectiveFunction) {

        int dimensions = objectiveFunction.getCoefficients().length;
        this.constraints = constraints;
        this.objectiveFunction = objectiveFunction;
        this.originalObjectiveFunction
                = new LinearObjectiveFunction(Arrays.copyOf(objectiveFunction.getCoefficients(),
                dimensions));
    }

    private void standardize(){
        int dimensions = objectiveFunction.getCoefficients().length;
        for(int j=0;j<constraints.length;j++){

            if(constraints[j].getConstraintType() == ConstraintType.LE){
                for(int i=0;i<dimensions;i++){
                    constraints[j].getCoefficients()[i]*=-1;
                }
                constraints[j].setRhs(-constraints[j].getRhs());
            }
        }

        for(int i=0;i<dimensions;i++){
            double coefficient = originalObjectiveFunction.getCoefficients()[i];
            if(coefficient<=0){
                objectiveFunction.getCoefficients()[i]*=-1;
                for(int j=0;j<constraints.length;j++){
                    constraints[j].getCoefficients()[i]*=-1;
                    constraints[j].setRhs(constraints[j].getRhs() + coefficient);
                }
            }
        }
    }
}
