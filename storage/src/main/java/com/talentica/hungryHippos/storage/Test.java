package com.talentica.hungryHippos.storage;

import org.apache.commons.math3.optim.PointValuePair;
import org.apache.commons.math3.optim.linear.*;
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType;

import java.util.Arrays;

/**
 * Created by debasishc on 31/7/15.
 * Demonstrating usage of apache math simplex solver
 */
public class Test {

    public static void main(String [] args){
        LinearObjectiveFunction objective = new LinearObjectiveFunction(new double[]{2,3,1}, 0.0);
        LinearConstraint constraint1 = new LinearConstraint(new double[]{1,1,1}, Relationship.LEQ,5.0);
        LinearConstraint constraint2 = new LinearConstraint(new double[]{1,2,0}, Relationship.LEQ,9.0);
        LinearConstraintSet constraintSet =
                new LinearConstraintSet(constraint1, constraint2);
        SimplexSolver solver = new SimplexSolver();

        PointValuePair result =
                solver.optimize(objective, constraintSet, GoalType.MAXIMIZE, new NonNegativeConstraint(false));
        System.out.println(Arrays.toString(result.getPoint()));
        System.out.println(result.getValue());


    }
}
