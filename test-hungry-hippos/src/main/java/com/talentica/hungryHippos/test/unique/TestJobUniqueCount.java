package com.talentica.hungryHippos.test.unique;

import com.talentica.hungryHippos.client.domain.Work;
import com.talentica.hungryHippos.test.sum.SumJob;

/**
 * Created by debasishc on 5/10/15.
 */
public class TestJobUniqueCount extends SumJob {

  private static final long serialVersionUID = 3943488225202378443L;
  static int jobId = 0;
  public TestJobUniqueCount(int[] dimensions, int valueIndex) {
    super(dimensions, valueIndex,jobId++);
  }

  @Override
  public Work createNewWork() {
    return new UniqueCountWork(dimensions, valueIndex,jobId);
  }

}
