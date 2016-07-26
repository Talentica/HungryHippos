package com.talentica.hungryHippos.storage;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.utility.PathUtil;

/**
 * This test class is written for testing the methods written in FileDataStore.java
 * 
 * @author sudarshans
 *
 */
public class FileDataStoreTest {

  private static FileDataStore fileDataStore = null;
  private static final DataDescription dataDescription = new FieldTypeArrayDataDescription(1);
  private static final int numDimensions = 2;

  @BeforeClass
  public static void setUp() throws Exception {
    fileDataStore = new FileDataStore(numDimensions, dataDescription,"hungryHippoFilePath","0");
  }

  /**
   * This testcase has an issue, the parent folder is not present it will create
   * fileNotFoundException.
   */
  @Test
  public void testStoreRow() {
    int storeId = 1;
    String test = "test";
    ByteBuffer row = ByteBuffer.allocate(50);
    byte[] raw = test.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    fileDataStore.storeRow(storeId, row, raw);
    try {
      String loc = new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath() + File.separator + "data"
          + File.separator + "data_" + 1;
      File file = new File(loc);

      assertTrue(file.exists());
    } catch (IOException e) {
      assertTrue(false);
    }
  }

  /**
   * This method test getStoreAccess(int keyId)
   * 
   * @throws JAXBException
   * @throws IOException
   * @throws InterruptedException
   * @throws KeeperException
   * @throws FileNotFoundException
   * @throws ClassNotFoundException
   */
  @Test
  public void testGetStoreAccess() throws ClassNotFoundException, FileNotFoundException,
      KeeperException, InterruptedException, IOException, JAXBException {
    StoreAccess storeAccess = fileDataStore.getStoreAccess(1);
    assertNotNull(storeAccess);
  }

  @Test
  public void testSync() {
    fileDataStore.sync();
    try {
      String loc = new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath() + File.separator + "data"
          + File.separator + "data_" + 1;
      File file = new File(loc);

      assertTrue(file.length() > 0);
    } catch (IOException e) {
      assertTrue(false);
    }

  }

  @AfterClass
  public static void tearDown() {
    fileDataStore = null;
  }

}
