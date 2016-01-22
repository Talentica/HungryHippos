package com.talentica.hungryHippos.test.unique;

import java.util.HashSet;

import com.talentica.hungryHippos.accumulator.ExecutionContext;
import com.talentica.hungryHippos.test.sum.SumWork;
import com.talentica.hungryHippos.utility.marshaling.MutableCharArrayString;

/**
 * Created by debasishc on 5/10/15.
 */
public class UniqueCountWork extends SumWork{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	HashSet<CharSequence> uniqueValues = new HashSet<>();
    private static int workerId = 0;
    public UniqueCountWork(int[] dimensions, int primaryDimension, int valueIndex) {
        super(dimensions,primaryDimension,valueIndex,String.valueOf(workerId++));
    }


    @Override
    public void processRow(ExecutionContext executionContext) {
        MutableCharArrayString v =  executionContext.getString(valueIndex);
        uniqueValues.add(v.clone());

    }

    @Override
    public void calculate(ExecutionContext executionContext) {
        executionContext.saveValue(valueIndex +" : "+uniqueValues.size());
    }
}
