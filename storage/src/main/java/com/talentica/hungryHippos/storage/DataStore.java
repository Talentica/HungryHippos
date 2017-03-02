package com.talentica.hungryHippos.storage;

/**
 * Created by debasishc on 27/8/15.
 */
public interface DataStore {
  void storeRow(String storeId, byte[] raw);

  void storeRow(int index, byte[] raw);

  public void sync();

  String getHungryHippoFilePath();

}
