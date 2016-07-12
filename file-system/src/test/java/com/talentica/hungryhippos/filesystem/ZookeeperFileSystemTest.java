package com.talentica.hungryhippos.filesystem;

import static org.junit.Assert.*;

import java.io.File;
import java.util.concurrent.CountDownLatch;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;
import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.property.Property;
import com.talentica.hungryHippos.coordination.property.ZkProperty;

@Ignore
@RunWith(PowerMockRunner.class)
@PrepareForTest({ NodesManagerContext.class, CoordinationApplicationContext.class })
@SuppressStaticInitializationFor({ "com.talentica.hungryHippos.coordination.domain.NodesManagerContext" })
public class ZookeeperFileSystemTest {

	private ZookeeperFileSystem system;

	private NodesManager nodesManager;
	private Property<ZkProperty> zkProperty;

	public void setUp() {
		PowerMockito.mockStatic(NodesManagerContext.class);
		PowerMockito.mockStatic(CoordinationApplicationContext.class);

		nodesManager = Mockito.mock(NodesManager.class);
		zkProperty = Mockito.mock(Property.class);
	}

	@Test
	public void testCreateZnode() {
		String testFolder = "test";
		String path = ZookeeperFileSystem.createZnode(testFolder);
		assertNotNull(path);
	}

	@Test
	public void testCreateZnodeArg2() {
		String testFolder = "test1";
		CountDownLatch signal = new CountDownLatch(1);
		String path = ZookeeperFileSystem.createZnode(testFolder, signal);
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
		String path = ZookeeperFileSystem.createZnode(testFolder, "testing");
		assertNotNull(path);
	}

	@Test
	public void testCheckZnodeExists() {
		boolean flag = ZookeeperFileSystem.checkZnodeExists("test2");
		assertTrue(flag);
	}

	@Test
	public void testCheckZnodeExistsNegative() {
		boolean flag = ZookeeperFileSystem.checkZnodeExists("test3");
		assertFalse(flag);
	}

	@Test
	public void testSetData() {
		ZookeeperFileSystem.setData("test1", "data Passed");

	}

	@Test
	public void testGetData() {
		String s = ZookeeperFileSystem.getData("test1");
		assertNotNull(s);
	}

	@Test
	public void testUpdateFSBlockMetaData() {
		setUp();
		try {
			String fileZKNode = "input";
			String fileSystemRootNodeZKPath = "/rootnode/filesystem";
			String dataFileZKNode = "0";
			long datafileSize = 1000L;
			String nodeIp = "localhost";
			String fileNodeZKPath = fileSystemRootNodeZKPath + File.separator + fileZKNode;
			String nodeIpZKPath = fileNodeZKPath + File.separator + nodeIp;

			PowerMockito.when(NodesManagerContext.getNodesManagerInstance()).thenReturn(nodesManager);
			PowerMockito.when(CoordinationApplicationContext.getZkProperty()).thenReturn(zkProperty);

			Mockito.when(nodesManager.checkNodeExists(fileNodeZKPath)).thenReturn(false);
			Mockito.when(nodesManager.checkNodeExists(nodeIpZKPath)).thenReturn(false);
			Mockito.when(nodesManager.getObjectFromZKNode(fileNodeZKPath)).thenReturn("0");
			Mockito.doNothing().when(nodesManager).createPersistentNode(Mockito.anyString(), new CountDownLatch(1),
					Mockito.eq(Mockito.any()));
			Mockito.when(zkProperty.getValueByKey(FileSystemConstants.ROOT_NODE)).thenReturn(fileSystemRootNodeZKPath);

			ZookeeperFileSystem.updateFSBlockMetaData(fileZKNode, nodeIp, dataFileZKNode, datafileSize);

			assertTrue(true);
		} catch (Exception e) {
			e.printStackTrace();
			assertTrue(false);
		}
	}

}
