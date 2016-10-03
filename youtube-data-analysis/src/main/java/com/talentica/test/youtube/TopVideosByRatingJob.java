package com.talentica.test.youtube;

import java.io.Serializable;
import java.util.Arrays;

import com.talentica.hungryHippos.client.domain.Work;
import com.talentica.hungryHippos.client.job.Job;

public class TopVideosByRatingJob implements Job, Serializable {

  private static final long serialVersionUID = -8299385914889558642L;

  protected int[] dimensions = new int[] {3};

  private int valueIndex = 6;

  @Override
  public Work createNewWork() {
    return new TopVideosWork(dimensions);
  }

  @Override
  public int[] getDimensions() {
    return dimensions;
  }

  @Override
  public String toString() {
    if (dimensions != null) {
      return "\nTopVideosByRatingJob{{dimensions:"
          + Arrays.toString(dimensions) + ", valueIndex:" + valueIndex + "}}";
    }
    return super.toString();
  }

}
