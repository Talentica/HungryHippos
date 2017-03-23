/*******************************************************************************
 * Copyright [2017] [Talentica Software Pvt. Ltd.]
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
package com.talentica.hungryHippos.test.median;

import java.io.Serializable;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

public final class MedianCalculator implements Serializable {

	private static final long serialVersionUID = 1L;

	private DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();

	public void addValue(Double value) {
		descriptiveStatistics.addValue(value);
	}

	public void addValue(Long value) {
		descriptiveStatistics.addValue(value);
	}
	
	public void addValue(Integer value) {
      descriptiveStatistics.addValue(value);
  }

	public Double calculate() {
		return descriptiveStatistics.getPercentile(50);
	}

	public void clear() {
		descriptiveStatistics.clear();
	}

}
