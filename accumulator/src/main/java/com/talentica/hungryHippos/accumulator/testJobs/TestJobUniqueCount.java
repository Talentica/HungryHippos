package com.talentica.hungryHippos.accumulator.testJobs;

import com.talentica.hungryHippos.accumulator.Work;

/**
 * Created by debasishc on 5/10/15.
 */
public class TestJobUniqueCount extends TestJob {
    public TestJobUniqueCount(int[] dimensions, int primaryDimension, int valueIndex) {
        super(dimensions,primaryDimension,valueIndex);
    }

    @Override
    public Work createNewWork() {
        return new TestWorkUniqueCount(dimensions,primaryDimension,valueIndex);
    }

}
