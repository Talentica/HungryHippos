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
package com.talentica.hungryHippos.test.knn;

import java.io.Serializable;

import com.talentica.hungryHippos.client.domain.Work;
import com.talentica.hungryHippos.client.job.Job;

public class KNNJob implements Job,Serializable{

  
  /**
   * 
   */
  private static final long serialVersionUID = 3820732324776854610L;
  protected int [] dimensions;
  protected int latitudeIndex;
  protected int longitudeIndex;
  protected int k;
  private int jobId;

  public KNNJob(){}
  
  public KNNJob(int [] dimensions,int latitudeIndex,int longitudeIndex,int k,int jobId){
    this.dimensions = dimensions;
    this.latitudeIndex = latitudeIndex;
    this.longitudeIndex = longitudeIndex;
    this.k = k;
    this.jobId = jobId;
  }
  @Override
  public Work createNewWork() {
    return new KNNWork(dimensions,latitudeIndex,longitudeIndex,k,jobId);
  }

  @Override
  public int[] getDimensions() {
    return dimensions;
  }

  @Override
  public int getJobId() {
    return jobId;
  }

}
