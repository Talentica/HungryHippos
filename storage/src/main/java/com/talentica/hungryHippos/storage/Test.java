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
        dataDescription.addFieldType(DataLocator.DataType.STRING,10);
        dataDescription.addFieldType(DataLocator.DataType.STRING,10);

        byte[] buf = new byte[dataDescription.getSize()];
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
        buf[10]='a';
        buf[11]='s';
        buf[12]='d';
        buf[13]=0;
        buf[14]=0;
        buf[15]=0;
        buf[20]='a';
        buf[21]=0;
        buf[22]=0;

        DynamicMarshal marshal = new DynamicMarshal(dataDescription);
//        System.out.println(marshal.readValue(0, ByteBuffer.wrap(buf)));
//        System.out.println(marshal.readValue(1, ByteBuffer.wrap(buf)));
//        System.out.println(marshal.readValue(2, ByteBuffer.wrap(buf)));
//        System.out.println(marshal.readValue(3, ByteBuffer.wrap(buf)));
//        System.out.println(marshal.readValue(4, ByteBuffer.wrap(buf)));
//        System.out.println(marshal.readValue(5, ByteBuffer.wrap(buf)));
        long start = System.currentTimeMillis();
        for(int i=0;i<10_000_000;i++) {
            marshal.readValue(0, ByteBuffer.wrap(buf));
            marshal.readValue(1, ByteBuffer.wrap(buf));
            marshal.readValue(2, ByteBuffer.wrap(buf));
            marshal.readValue(3, ByteBuffer.wrap(buf));
            marshal.readValue(4, ByteBuffer.wrap(buf));
            marshal.readValue(5, ByteBuffer.wrap(buf));
        }
        long end = System.currentTimeMillis();
        System.out.println(end-start);

    }
}
