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
package com.talentica.hungryHippos.test.sum.lng;

import java.io.Serializable;

import com.talentica.hungryHippos.client.domain.ExecutionContext;
import com.talentica.hungryHippos.client.domain.Work;

/**
 * Created by debasishc on 9/9/15.
 */
public class SumWork implements Work, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5931349264723731947L;
	protected int[] dimensions;
	protected int valueIndex;
	private long sum;
	private int jobId;

  public SumWork(int[] dimensions, int valueIndex,int jobId) {
		this.dimensions = dimensions;
		this.valueIndex = valueIndex;
		this.jobId = jobId;
	}

	@Override
	public void processRow(ExecutionContext executionContext) {
		sum = sum + ((Long) executionContext.getValue(valueIndex));
	}

	@Override
	public void calculate(ExecutionContext executionContext) {
		executionContext.saveValue(valueIndex, sum, "Sum");
	}

	@Override
	public void reset() {
		sum = 0;
	}

  @Override
  public int getJobId() {
   return jobId;
  }

}
