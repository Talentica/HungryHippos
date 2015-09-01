package com.talentica.hungryHippos.storage;

import com.talentica.hungryHippos.utility.marshaling.DataLocator;
import com.talentica.hungryHippos.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.utility.marshaling.FieldTypeArrayDataDescription;
import org.apache.commons.math3.optim.PointValuePair;
import org.apache.commons.math3.optim.linear.*;
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Created by debasishc on 31/7/15.
 * Demonstrating usage of apache math simplex solver
 */
public class Test {

    public static void main(String [] args){
//        LinearObjectiveFunction objective = new LinearObjectiveFunction(new double[]{2,3,1}, 0.0);
//        LinearConstraint constraint1 = new LinearConstraint(new double[]{1,1,1}, Relationship.LEQ,5.0);
//        LinearConstraint constraint2 = new LinearConstraint(new double[]{1,2,0}, Relationship.LEQ,9.0);
//        LinearConstraintSet constraintSet =
//                new LinearConstraintSet(constraint1, constraint2);
//        SimplexSolver solver = new SimplexSolver();
//
//        PointValuePair result =
//                solver.optimize(objective, constraintSet, GoalType.MAXIMIZE, new NonNegativeConstraint(false));
//        System.out.println(Arrays.toString(result.getPoint()));
//        System.out.println(result.getValue());


        FieldTypeArrayDataDescription dataDescription = new FieldTypeArrayDataDescription();
        dataDescription.addFieldType(DataLocator.DataType.BYTE,0);
        dataDescription.addFieldType(DataLocator.DataType.BYTE,0);
        dataDescription.addFieldType(DataLocator.DataType.INT,0);
        dataDescription.addFieldType(DataLocator.DataType.INT,0);

        byte[] buf = new byte[10];
        buf[0]=5;
        buf[1]=5;
        buf[2]=0;
        buf[3]=0;
        buf[4]=1;
        buf[5]=0;
        buf[6]=0;
        buf[7]=0;
        buf[8]=2;
        buf[9]=0;

        DynamicMarshal marshal = new DynamicMarshal(dataDescription);
        System.out.println(marshal.readValue(0, ByteBuffer.wrap(buf)));
        System.out.println(marshal.readValue(1, ByteBuffer.wrap(buf)));
        System.out.println(marshal.readValue(2, ByteBuffer.wrap(buf)));
        System.out.println(marshal.readValue(3, ByteBuffer.wrap(buf)));


    }
}
