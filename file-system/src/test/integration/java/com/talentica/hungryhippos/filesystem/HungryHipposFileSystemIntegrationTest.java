package com.talentica.hungryhippos.filesystem;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.util.concurrent.CountDownLatch;

import javax.xml.bind.JAXBException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
// import org.mockito.Mockito;
// import org.powermock.api.mockito.PowerMockito;

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.property.Property;
import com.talentica.hungryHippos.coordination.property.ZkProperty;
import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.client.ClientConfig;
import com.talentica.hungryhippos.filesystem.client.DataRetrieverClient;

/*
 * @RunWith(PowerMockRunner.class)
 * 
 * @PrepareForTest({NodesManagerContext.class, CoordinationApplicationContext.class})
 * 
 * @SuppressStaticInitializationFor({
 * "com.talentica.hungryHippos.coordination.domain.NodesManagerContext"})
 */
public class HungryHipposFileSystemIntegrationTest {

  private HungryHipposFileSystem hhfs = null;
  private String clientConfig = "";

  private HungryHippoCurator curator;
  private Property<ZkProperty> zkProperty;

  @Before
  public void setUp() throws FileNotFoundException, JAXBException {
    /*
     * PowerMockito.mockStatic(NodesManagerContext.class);
     * PowerMockito.mockStatic(CoordinationApplicationContext.class);
     * 
     * nodesManager = Mockito.mock(NodesManager.class); zkProperty = Mockito.mock(Property.class);
     */

 //   curator = HungryHippoCurator.getInstance("");

//
 //   hhfs = HungryHipposFileSystem.getInstance();
  }

  @After
  public void tearDown() {
    hhfs = null;
  }

  @Test
  public void testCreateZnode() {
    String testFolder = "test";
    String path = hhfs.createZnode(testFolder);
    assertNotNull(path);
  }

  @Test
  public void testCreateZnodeArg2() {
    String testFolder = "test1";
    CountDownLatch signal = new CountDownLatch(1);
    String path = hhfs.createZnode(testFolder, signal);
    try {
      signal.await();
    } catch (InterruptedException e) {
      assertFalse(false);
    }
    assertNotNull(path);
  }

  @Test
  public void testCreateZnodeArg3() {
    String testFolder = "test2";
    String path = hhfs.createZnode(testFolder, "testing");
    assertNotNull(path);
  }

  @Test
  public void testCheckZnodeExists() {
    boolean flag = hhfs.checkZnodeExists("test2");
    assertTrue(flag);
  }

  @Test
  public void testCheckZnodeExistsNegative() {
    boolean flag = hhfs.checkZnodeExists("test3");
    assertFalse(flag);
  }

  @Test
  public void testSetData() {
    hhfs.setData("test1", "data Passed");

  }



  @Test
  public void testUpdateFSBlockMetaData() throws FileNotFoundException, JAXBException {
    try {
      String fileZKNode = "input";
      String fileSystemRootNodeZKPath = "/rootnode/filesystem";
      int dataFolderZKNode = 0;
      long datafileSize = 1000L;
      int nodeId = 0;
      String fileName = "0";
      String fileNodeZKPath =
          fileSystemRootNodeZKPath + FileSystemConstants.ZK_PATH_SEPARATOR + fileZKNode;
      hhfs.createZnode(fileNodeZKPath, FileSystemConstants.IS_A_FILE);

      hhfs.updateFSBlockMetaData(fileZKNode, nodeId, dataFolderZKNode, fileName, datafileSize);
      assertTrue(true);
    } catch (Exception e) {
      e.printStackTrace();
      assertTrue(false);
    }
  }
  
  @Test
  public void downloadOutput() throws Exception{
    String clientConfigFilePath = "/home/sudarshans/config/client-config.xml";
    ClientConfig clientConfig =
        JaxbUtil.unmarshalFromFile(clientConfigFilePath, ClientConfig.class);
    String connectString = clientConfig.getCoordinationServers().getServers();
    int sessionTimeOut = Integer.valueOf(clientConfig.getSessionTimout());
    HungryHippoCurator.getInstance(connectString, sessionTimeOut);
    System.out.println("Here");
    DataRetrieverClient.getHungryHippoData("/dir/output1", "/home/sudarshans/server/out", 0);
  }


}
