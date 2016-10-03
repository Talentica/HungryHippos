package com.talentica.hungryHippos.storage;

import java.io.IOException;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;

/**
 * Created by debasishc on 27/8/15.
 */
public interface DataStore {
  void storeRow(int storeId, byte[] raw);

  /**
   * A moving stream.
   *
   * @return
   */
  public StoreAccess getStoreAccess(int keyId) throws ClassNotFoundException,
      KeeperException, InterruptedException, IOException, JAXBException;

  public void sync();

  String getHungryHippoFilePath();

}
