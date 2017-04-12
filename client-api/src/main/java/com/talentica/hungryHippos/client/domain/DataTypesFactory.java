package com.talentica.hungryHippos.client.domain;

public class DataTypesFactory {

  public static DataTypes getNewInstance(DataTypes dataType){
    if(dataType == null){
      return null;
    }else if(dataType instanceof MutableCharArrayString){
      MutableCharArrayString charArray = new MutableCharArrayString(dataType.getLength());
      charArray.addValue(dataType.toString());
      return charArray;
    }else{
      return dataType.clone();
    }
  }
  
  public static DataTypes getNewInstance(DataTypes dataType, int splitIndex){
    if(dataType == null){
      return null;
    }else if(dataType instanceof MutableCharArrayString){
      MutableCharArrayString charArray = new MutableCharArrayString(dataType.getLength());
      charArray.addValue(dataType.toString());
      charArray.setSplitIndex(splitIndex);
      return charArray;
    }else{
      DataTypes newInstance = dataType.clone();
      newInstance.setSplitIndex(splitIndex);
      return newInstance;
    }
  }
}
