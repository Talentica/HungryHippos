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
