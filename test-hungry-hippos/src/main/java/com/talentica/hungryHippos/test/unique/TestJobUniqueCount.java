package com.talentica.hungryHippos.test.unique;

import com.talentica.hungryHippos.client.domain.Work;
import com.talentica.hungryHippos.test.sum.SumJob;

/**
 * Created by debasishc on 5/10/15.
 */
public class TestJobUniqueCount extends SumJob {
    public TestJobUniqueCount(int[] dimensions, int primaryDimension, int valueIndex) {
        super(dimensions,primaryDimension,valueIndex,0);// zero added . need to be removed
    }

    @Override
    public Work createNewWork() {
        return new UniqueCountWork(dimensions,primaryDimension,valueIndex);
    }

}
