/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.talentica.hungryHippos.test.median.lng;

import java.io.Serializable;
import java.util.Arrays;

import com.talentica.hungryHippos.client.domain.Work;
import com.talentica.hungryHippos.client.job.Job;

/**
 * Created by debasishc on 9/9/15.
 */
public class MedianJob implements Job, Serializable {

  private static final long serialVersionUID = -4111336293020419218L;
  protected int[] dimensions;
  protected int valueIndex;
 protected int jobId;
  public MedianJob() {}

  public MedianJob(int[] dimensions, int primaryDimension, int valueIndex,int jobId) {
    this.dimensions = dimensions;
    this.valueIndex = valueIndex;
  }


  @Override
  public Work createNewWork() {
    return new MedianWork(dimensions, valueIndex,jobId);
  }

  @Override
  public int[] getDimensions() {
    return dimensions;
  }

  public long getMemoryFootprint(long rowCount) {
    return 58 * rowCount;
  }

  @Override
  public String toString() {
    if (dimensions != null) {
      return "\nMedianJob{{dimensions:" + Arrays.toString(dimensions) + ", valueIndex:" + valueIndex
          + "}}";
    }
    return super.toString();
  }

  public int getIndex() {
    return valueIndex;
  }

  @Override
  public int getJobId() {
    return jobId;
  }

}
