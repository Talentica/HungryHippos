package com.talentica.hungryHippos.utility;

public class Counter {

  private long count;
  
  private long startCount;
  
  private long maxCount;
  
  public Counter(long maxCount){
      this.startCount = -1;
      this.count = this.startCount;
      this.maxCount = maxCount;
  }
  
  public Counter(long startCount,long maxCount){
      this.startCount = startCount-1;
      this.count = this.startCount;
      this.maxCount = maxCount;
  }
  
  public long getNextCount(){
      count++;
      if(count > maxCount){
          count = startCount + (count % maxCount);
      }
      return count;
  }
  
}
