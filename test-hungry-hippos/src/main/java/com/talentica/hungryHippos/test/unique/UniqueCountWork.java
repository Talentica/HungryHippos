package com.talentica.hungryHippos.test.unique;

import java.util.HashSet;

import com.talentica.hungryHippos.client.domain.ExecutionContext;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.test.sum.SumWork;

/**
 * Created by debasishc on 5/10/15.
 */
public class UniqueCountWork extends SumWork {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  HashSet<CharSequence> uniqueValues = new HashSet<>();

  public UniqueCountWork(int[] dimensions, int valueIndex) {
    super(dimensions, valueIndex);
  }


  @Override
  public void processRow(ExecutionContext executionContext) {
    MutableCharArrayString v = executionContext.getString(valueIndex);
    uniqueValues.add(v.clone());

  }

  @Override
  public void calculate(ExecutionContext executionContext) {
    executionContext.saveValue(valueIndex, " : " + uniqueValues.size(), "Unique ");
  }
}
