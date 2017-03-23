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
import java.util.ArrayList;
import java.util.List;

import com.talentica.hungryHippos.client.domain.ExecutionContext;
import com.talentica.hungryHippos.client.domain.Work;

public class ClosePointsWork implements Work, Serializable{

  /**
   * 
   */
  private static final long serialVersionUID = 5690254822184241977L;
  protected int[] dimensions;
  protected int latitudeIndex;
  protected int longitudeIndex;
  protected double latitude;
  protected double longitude;
  private int jobId;
  protected List<Record> records = new ArrayList<Record>();
  
  public ClosePointsWork(int[] dimensions, int latitudeIndex, int longitudeIndex,
      double latitude, double longitude,int jobId){
    this.dimensions = dimensions;
    this.latitudeIndex = latitudeIndex;
    this.longitudeIndex = longitudeIndex;
    this.latitude = latitude;
    this.longitude = longitude;
    this.jobId = jobId;
  }

  @Override
  public void processRow(ExecutionContext executionContext) {
    double latitude = (double) executionContext.getValue(latitudeIndex);
    double longitude = (double) executionContext.getValue(longitudeIndex);
    double dist = distance(this.latitude,this.longitude,latitude,longitude);
    
    if(dist <= 5.0 && dist >= -5.0){
      records.add(new Record(executionContext.getString(0).toString(),
          executionContext.getString(1).toString(),
          executionContext.getString(2).toString(),
          executionContext.getString(3).toString(),
         (int) executionContext.getValue(4),
         (double) executionContext.getValue(5),
         (double) executionContext.getValue(6),
         (double) executionContext.getValue(7)));
    }
  }

  @Override
  public void calculate(ExecutionContext executionContext) {
    for(Record record : records){
      executionContext.saveValue(record);
    }
  }

  @Override
  public void reset() {
    records = new ArrayList<Record>();
  }
  
  public static double distance(double lat1, double lon1, double lat2, double lon2) {
    double theta = lon1 - lon2;
    double dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2)) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.cos(deg2rad(theta));
    dist = Math.acos(dist);
    dist = rad2deg(dist);
    dist = dist * 60 * 1.1515;
    if(dist <= 5.0 && dist >= -5.0){
      System.out.println("latitude : " + lat2 + " Longitude : " + lon2);
    }
    return (dist);
  }
  
  private static double deg2rad(double deg) {
    return (deg * Math.PI / 180.0);
  }
  
  private static double rad2deg(double rad) {
    return (rad * 180 / Math.PI);
  }

  @Override
  public int getJobId() {
  return jobId;
  }

}
