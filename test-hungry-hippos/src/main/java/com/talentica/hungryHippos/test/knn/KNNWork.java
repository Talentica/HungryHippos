package com.talentica.hungryHippos.test.knn;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

import com.talentica.hungryHippos.client.domain.ExecutionContext;
import com.talentica.hungryHippos.client.domain.Work;

public class KNNWork implements Work, Serializable{

  /**
   * 
   */
  private static final long serialVersionUID = 5515151408371415785L;
  protected int[] dimensions;
  protected int latitudeIndex;
  protected int longitudeIndex;
  protected int k;
  private int jobId;
  private double minLat = Double.MAX_VALUE;
  private double maxLat = Double.MIN_VALUE;
  private double minLong = Double.MAX_VALUE;
  private double maxLong = Double.MIN_VALUE;
  private int divisionFactor = 5;
  private double minRadius;
  private static double radius[] = new double[4];
  IndexCalculator calc = new IndexCalculator(divisionFactor, divisionFactor);
  @SuppressWarnings("unchecked")
  private List<Record>[][] map = new List[divisionFactor][divisionFactor]; 
  protected List<Record> records = new ArrayList<Record>();

  public KNNWork(int[] dimensions,int latitudeIndex,int longitudeIndex,int k, int jobId){
    this.dimensions = dimensions;
    this.latitudeIndex = latitudeIndex;
    this.longitudeIndex = longitudeIndex;
    this.k = k;
    this.jobId = jobId;
  }
  
  @Override
  public void processRow(ExecutionContext executionContext) {
    if((double)executionContext.getValue(latitudeIndex) < minLat){
      minLat = Math.floor((double)executionContext.getValue(latitudeIndex));
    }
    if((double)executionContext.getValue(latitudeIndex) > maxLat){
      maxLat = Math.ceil((double)executionContext.getValue(latitudeIndex));
    }
    if((double)executionContext.getValue(longitudeIndex) < minLong){
      minLong = Math.floor((double)executionContext.getValue(longitudeIndex));
    }
    if((double)executionContext.getValue(longitudeIndex) > maxLong){
      maxLong = Math.ceil((double)executionContext.getValue(longitudeIndex));
    }
    records.add(new Record(executionContext.getString(0).toString(),
        executionContext.getString(1).toString(),
        executionContext.getString(2).toString(),
        executionContext.getString(3).toString(),
       (int) executionContext.getValue(4),
       (double) executionContext.getValue(5),
       (double) executionContext.getValue(6),
       (double) executionContext.getValue(7)));
  }

  @Override
  public void calculate(ExecutionContext executionContext) {
    guardBoundary();
    for(int i = 0; i < divisionFactor; i++){
      for(int j = 0; j < divisionFactor; j++){
        map[i][j] = new ArrayList<Record>();
      }
    }
    while(records.size() != 0){
      Record record= records.remove(0);
      int latDivision = getIndex(record.getLatitude(),minLat,maxLat,divisionFactor);
      int longDivision = getIndex(record.getLongitude(),minLong,maxLong,divisionFactor);
      map[latDivision][longDivision].add(record);
    }
    kNN(executionContext);
  }
  
  public void guardBoundary(){
    minLat = minLat - 1;
    maxLat = maxLat + 1;
    minLong = minLong - 1;
    maxLong = maxLong + 1;
  }
  
  private int getIndex(double value,double min,double max,int factor){
    return (int)((value - min) / ((max-min) / factor));
  }
  
  private void kNN(ExecutionContext executionContext){
    for(int i = 0; i < divisionFactor; i++){
      for(int j = 0; j < divisionFactor; j++){
        List<Record> list = map[i][j];
        for(Record refRecord : list){
         /* System.out.println("k nearest neighbour for latitude = "+ refRecord.getLatitude() +
              " longitude = "+refRecord.getLongitude()+"=====>");*/
          executionContext.saveValue("k nearest neighbour for latitude = "+ refRecord.getLatitude() +
              " longitude = "+refRecord.getLongitude()+"=====>");
          PriorityQueue<RecordComparable> resultantRecords = new PriorityQueue<RecordComparable>(k);
          getCircularBoundary(refRecord.getLatitude(),refRecord.getLongitude(),i,j);
          populateQueue(refRecord,list,resultantRecords);
          int leftI = i;
          int leftJ = j;
          int rightI = i;
          int rightJ = j;
          while(resultantRecords.size() < k || resultantRecords.peek().getDistance() > minRadius){
              Point topLeft = new Point(leftI,leftJ);
              Point bottomRight = new Point(rightI,rightJ);
              Set<Point> points = calc.getIndexes(topLeft, bottomRight);
              for(Point point : points){
                  List<Record> neighbourList = map[point.getI()][point.getJ()];
                  populateQueue(refRecord,neighbourList,resultantRecords);
              }
              updateCircularBoundary();
              leftI--;
              leftJ--;
              rightI++;
              rightJ++;
              if(leftI < 0 && leftJ < 0 && rightI >= divisionFactor && rightJ >= divisionFactor){
                break;
              }
              if(leftI < 0){
                  leftI = 0;
              }
              if(leftJ < 0){
                  leftJ = 0;
              }
              if(rightI >= divisionFactor){
                  rightI = divisionFactor - 1;
              }
              if(rightJ >= divisionFactor){
                  rightJ = divisionFactor - 1;
              }
          }
          for(RecordComparable result : resultantRecords){
            //System.out.println(result.getRecord());
            executionContext.saveValue(result.getRecord());
          }
        }
      }
    }
  }
  
  private double getCircularBoundary(double lat,double lon,int i, int j){
    radius[0] = lat - (minLat + (((maxLat-minLat)/divisionFactor)*i));
    radius[1] = (((maxLat-minLat)/divisionFactor)) - radius[0];
    radius[2] = lon - (minLong + (((maxLong - minLong)/divisionFactor)*j));
    radius[3] = (((maxLong - minLong)/divisionFactor)) - radius[2];
    return getminRadius();
  }
  
  public double updateCircularBoundary(){
    radius[0] = radius[0] + ((maxLat-minLat)/divisionFactor);
    radius[1] = radius[1] + ((maxLat-minLat)/divisionFactor);
    radius[2] = radius[2] + ((maxLong - minLong)/divisionFactor);
    radius[3] = radius[3] + ((maxLong - minLong)/divisionFactor);
    return getminRadius();
  }
  
  private double getminRadius(){
    minRadius = radius[0];
    for(int x = 1; x < 4; x++){
        if(radius[x] < minRadius){
            minRadius = radius[x];
        }
    }
    return minRadius;
  }
  
  public void populateQueue(Record refRecord,List<Record> list,PriorityQueue<RecordComparable> resultantRecords){
    for(int i = 0; i < list.size(); i++){
        Record record = list.get(i);
        double dist = calculateDistance(refRecord.getLatitude(),refRecord.getLongitude(),
            record.getLatitude(),record.getLongitude());
        RecordComparable rec = new RecordComparable(record, dist);
        if (resultantRecords.size() < k) {
          resultantRecords.offer(rec);
        } else if(resultantRecords.size() != 0){
          if (resultantRecords.peek().compareTo(rec) < 0) {
            resultantRecords.poll();
            resultantRecords.offer(rec);
          }
        }
      }
  }
  
  public static double calculateDistance(double lat1, double lon1, double lat2, double lon2) {
    double theta = lon1 - lon2;
    double dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2)) +
            Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.cos(deg2rad(theta));
    dist = Math.acos(dist);
    dist = rad2deg(dist);
    dist = dist * 60 * 1.1515;
    return (dist);
  }
        
  private static double deg2rad(double deg) {
      return (deg * Math.PI / 180.0);
  }
        
  private static double rad2deg(double rad) {
      return (rad * 180 / Math.PI);
  }

  @Override
  public void reset() {
    records = null;
  }

  @Override
  public int getJobId() {
   return jobId;
  }

  
}
