package com.talentica.hungryHippos.accumulator.testJobs;

import com.talentica.hungryHippos.accumulator.Job;
import com.talentica.hungryHippos.accumulator.Work;

/**
 * Created by debasishc on 9/9/15.
 */
public class TestJob implements Job{
    private int [] dimensions;
    private int primaryDimension;
    private int valueIndex;

    public TestJob(int[] dimensions, int primaryDimension, int valueIndex) {
        this.dimensions = dimensions;
        this.primaryDimension = primaryDimension;
        this.valueIndex = valueIndex;
    }




    @Override
    public Work createNewWork() {
        return new TestWork(dimensions,primaryDimension,valueIndex);
    }

    @Override
    public int[] getDimensions() {
        return dimensions;
    }

    @Override
    public int getPrimaryDimension() {
        return primaryDimension;
    }
}
