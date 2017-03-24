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
