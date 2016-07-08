package com.talentica.hungryhippos.filesystem;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import javax.xml.bind.JAXBException;

import org.junit.Before;
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
import com.talentica.hungryhippos.config.client.CoordinationServers;
import com.talentica.hungryhippos.config.client.ObjectFactory;
import com.talentica.hungryhippos.filesystem.client.DataRetrieverClient;
import com.talentica.hungryhippos.filesystem.property.FileSystemProperty;
import com.talentica.hungryhippos.filesystem.util.FileSystemUtils;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ NodesManagerContext.class, CoordinationApplicationContext.class, CoordinationApplicationContext.class,
		DataRetrieverClient.class, FileSystemUtils.class })
@SuppressStaticInitializationFor({ "com.talentica.hungryHippos.coordination.domain.NodesManagerContext" })
public class ZookeeperFileSystemTest {

	private ZookeeperFileSystem system;

	private NodesManager nodesManager;
	private Property<ZkProperty> zkProperty;

	@Before
	public void setUp() {
		PowerMockito.mockStatic(NodesManagerContext.class);
		PowerMockito.mockStatic(CoordinationApplicationContext.class);

		nodesManager = Mockito.mock(NodesManager.class);
		PowerMockito.spy(DataRetrieverClient.class);
		PowerMockito.spy(FileSystemUtils.class);

		nodesManager = Mockito.mock(NodesManager.class);
		zkProperty = Mockito.mock(Property.class);
	}

	@Test

	public void testGetDimensionOperand() {
		int dimensionOperand = FileSystemUtils.getDimensionOperand(1);
		assertEquals(dimensionOperand, 1);
		dimensionOperand = FileSystemUtils.getDimensionOperand(3);
		assertEquals(dimensionOperand, 4);
	}

	@Test
	public void testCreateDirectory() {
		String dirName = "TestDir";
		File directory = new File(dirName);
		FileSystemUtils.createDirectory(dirName);
		directory.deleteOnExit();
		assertTrue(directory.exists() && directory.isDirectory());
	}

	@Test
	public void testGetHungryHippoData() {
		try {
			String fileZKNode = "input";
			String outputDirName = System.getProperty("user.home") + "/data";
			int dimension = 2;
			String fileSystemRootNodeZKPath = "/rootnode/filesystem";
			List<String> dataFileNodes = Arrays.asList(new String[] { "0", "1", "2", "3" });
			String nodeIp = "localhost";
			String fileNodeZKPath = fileSystemRootNodeZKPath + File.separator + fileZKNode;
			String nodeIpZKPath = fileNodeZKPath + File.separator + nodeIp;
			List<String> nodeIps = new ArrayList<>();
			nodeIps.add(nodeIp);

			PowerMockito.doNothing().when(DataRetrieverClient.class, "retrieveDataBlocks", Mockito.anyString(),
					Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyLong());
			PowerMockito.doNothing().when(FileSystemUtils.class, "createDirectory", Mockito.anyString());
			PowerMockito.when(NodesManagerContext.getNodesManagerInstance()).thenReturn(nodesManager);
			PowerMockito.when(CoordinationApplicationContext.getZkProperty()).thenReturn(zkProperty);

			for (String dataFileNode : dataFileNodes) {
				Mockito.when(nodesManager.getObjectFromZKNode(nodeIpZKPath + File.separator + dataFileNode))
						.thenReturn("100");
			}
			Mockito.when(nodesManager.getChildren(fileNodeZKPath)).thenReturn(nodeIps);
			Mockito.when(nodesManager.getChildren(nodeIpZKPath)).thenReturn(dataFileNodes);
			Mockito.when(zkProperty.getValueByKey(FileSystemConstants.ROOT_NODE)).thenReturn(fileSystemRootNodeZKPath);

			DataRetrieverClient.getHungryHippoData(fileZKNode, outputDirName, dimension);

			assertTrue(true);
		} catch (Exception e) {
			e.printStackTrace();
			assertTrue(false);
		}

	}


	@Test
	public void testUpdateFSBlockMetaData() {

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

			ZookeeperFileSystem zookeeperFileSystem = new ZookeeperFileSystem();
			zookeeperFileSystem.updateFSBlockMetaData(fileZKNode, nodeIp, dataFileZKNode, datafileSize);

			assertTrue(true);
		} catch (Exception e) {
			e.printStackTrace();
			assertTrue(false);
		}
	}
}
