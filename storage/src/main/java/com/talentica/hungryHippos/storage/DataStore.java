package com.talentica.hungryHippos.storage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;

import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;

/**
 * Created by debasishc on 27/8/15.
 */
public interface DataStore {
  public void storeRow(int storeId, ByteBuffer row, byte[] raw);

  /**
   * A moving stream.
   * 
   * @return
   */
  public StoreAccess getStoreAccess(int keyId) throws ClassNotFoundException, FileNotFoundException,
      KeeperException, InterruptedException, IOException, JAXBException;

  public void sync();

  String getHungryHippoFilePath();
}
