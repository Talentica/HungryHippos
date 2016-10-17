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

  public KNNJob(){}
  
  public KNNJob(int [] dimensions,int latitudeIndex,int longitudeIndex,int k){
    this.dimensions = dimensions;
    this.latitudeIndex = latitudeIndex;
    this.longitudeIndex = longitudeIndex;
    this.k = k;
  }
  @Override
  public Work createNewWork() {
    return new KNNWork(dimensions,latitudeIndex,longitudeIndex,k);
  }

  @Override
  public int[] getDimensions() {
    return dimensions;
  }

}
