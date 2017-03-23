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
package com.talentica.hungryHippos.test.closepoints;

import java.io.Serializable;
import java.util.Arrays;

import com.talentica.hungryHippos.client.domain.Work;
import com.talentica.hungryHippos.client.job.Job;

public class ClosePointsJob implements Job,Serializable{

  /**
   * 
   */
  private static final long serialVersionUID = 5430701283883963272L;
  protected int [] dimensions;
  protected int latitudeIndex;
  protected int longitudeIndex;
  protected double latitude;
  protected double longitude;
  private int  jobId;
  public ClosePointsJob(){}

  public ClosePointsJob(int[] dimensions, int latitudeIndex,
      int longitudeIndex, double latitude, double longitude,int jobId) {
    super();
    this.dimensions = dimensions;
    this.latitudeIndex = latitudeIndex;
    this.longitudeIndex = longitudeIndex;
    this.latitude = latitude;
    this.longitude = longitude;
    this.jobId = jobId;
  }



  @Override
  public Work createNewWork() {
    return new ClosePointsWork(dimensions,latitudeIndex,longitudeIndex,latitude,longitude,jobId);
  }

  @Override
  public int[] getDimensions() {
    return dimensions;
  }
  
  @Override
  public String toString() {
      if (dimensions != null) {
          return "\nClosePointsJob{{dimensions" + Arrays.toString(dimensions)
                  + ", latitudeIndex:" + latitudeIndex + ", longitudeIndex" + longitudeIndex +"}}";
      }
      return super.toString();
  }

  @Override
  public int getJobId() {
    return jobId;
  }
  
}
