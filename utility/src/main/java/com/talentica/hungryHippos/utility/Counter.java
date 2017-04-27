package com.talentica.hungryHippos.utility;

public class Counter {

  private int count;
    
  private int maxCount;
  
  public Counter(int maxCount){
      this.count = -1;
      this.maxCount = maxCount;
  }
  
  public int getNextCount(){
      count++;
      if(count > maxCount){
          count = 0;
      }
      return count;
  }
  
}
