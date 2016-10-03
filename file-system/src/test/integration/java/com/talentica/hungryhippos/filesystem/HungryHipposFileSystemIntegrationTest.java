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

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.property.Property;
import com.talentica.hungryHippos.coordination.property.ZkProperty;
import com.talentica.hungryHippos.utility.FileSystemConstants;

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

  private NodesManager nodesManager;
  private Property<ZkProperty> zkProperty;

  @Before
  public void setUp() throws FileNotFoundException, JAXBException {
    /*
     * PowerMockito.mockStatic(NodesManagerContext.class);
     * PowerMockito.mockStatic(CoordinationApplicationContext.class);
     * 
     * nodesManager = Mockito.mock(NodesManager.class); zkProperty = Mockito.mock(Property.class);
     */

    NodesManager nodeManager = NodesManagerContext.getNodesManagerInstance(
        "/home/sudarshans/RD/HH_NEW/HungryHippos/configuration-schema/src/main/resources/distribution/client-config.xml");

    hhfs = HungryHipposFileSystem.getInstance();
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
  public void testGetData() {
    String s = hhfs.getData("test1");

    String s1 = hhfs.getNodeData("test1");

    assertNotNull(s);
  }

  @Test
  public void testUpdateFSBlockMetaData() throws FileNotFoundException, JAXBException {

    try {
      String fileZKNode = "input";
      String fileSystemRootNodeZKPath = "/rootnode/filesystem";

      int dataFileZKNode = 0;
      long datafileSize = 1000L;
      String nodeIp = "localhost";
      String nodeId = "0";


      String fileNodeZKPath =
          fileSystemRootNodeZKPath + FileSystemConstants.ZK_PATH_SEPARATOR + fileZKNode;

      /*
       * PowerMockito.when(NodesManagerContext.getNodesManagerInstance()). thenReturn(nodesManager);
       * PowerMockito.when(CoordinationApplicationContext.getZkProperty()) .thenReturn(zkProperty);
       * 
       * Mockito.when(nodesManager.checkNodeExists(fileNodeZKPath)). thenReturn(false);
       * Mockito.when(nodesManager.checkNodeExists(nodeIpZKPath)). thenReturn(false);
       * Mockito.when(nodesManager.getObjectFromZKNode(fileNodeZKPath)). thenReturn("0");
       * Mockito.doNothing().when(nodesManager).createPersistentNode( Mockito.anyString(), new
       * CountDownLatch(1), Mockito.eq(Mockito.any()));
       * Mockito.when(zkProperty.getValueByKey(FileSystemConstants. ROOT_NODE))
       * .thenReturn(fileSystemRootNodeZKPath);
       */

      hhfs.updateFSBlockMetaData(fileZKNode, nodeIp, dataFileZKNode, nodeId, datafileSize);
      hhfs.updateFSBlockMetaData(fileZKNode, Integer.parseInt(nodeId), datafileSize);

      assertTrue(true);
    } catch (Exception e) {
      e.printStackTrace();
      assertTrue(false);
    }
  }

}
