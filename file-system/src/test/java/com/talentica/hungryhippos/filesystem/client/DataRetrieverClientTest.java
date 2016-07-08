package com.talentica.hungryhippos.filesystem.client;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.property.Property;
import com.talentica.hungryHippos.coordination.property.ZkProperty;
import com.talentica.hungryhippos.config.filesystem.FileSystemConfig;
import com.talentica.hungryhippos.filesystem.FileSystemConstants;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import com.talentica.hungryhippos.filesystem.util.FileSystemUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * This is a Test class for DataRetrieverClient Created by rajkishoreh on
 * 11/7/16.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ NodesManagerContext.class, CoordinationApplicationContext.class, DataRetrieverClient.class,
		FileSystemContext.class, FileSystemUtils.class })
@SuppressStaticInitializationFor({ "com.talentica.hungryHippos.coordination.domain.NodesManagerContext" })
public class DataRetrieverClientTest {

	private NodesManager nodesManager;
	private FileSystemConfig fileSystemConfig;
	private Property<ZkProperty> zkProperty;

	@Before
	public void setUp() {
		PowerMockito.mockStatic(NodesManagerContext.class);
		PowerMockito.mockStatic(CoordinationApplicationContext.class);
		PowerMockito.mockStatic(FileSystemContext.class);
		PowerMockito.spy(DataRetrieverClient.class);
		PowerMockito.spy(FileSystemUtils.class);

		nodesManager = Mockito.mock(NodesManager.class);
		fileSystemConfig = Mockito.mock(FileSystemConfig.class);
		zkProperty = Mockito.mock(Property.class);
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
	public void testRequestDataBlocks() {
		try {
			String fileZKNode = "input";
			String outputDirName = System.getProperty("user.home") + "/data";
			String dataFilePaths = "1,3";
			String nodeIp = "localhost";
			long dataSize = 1000L;

			PowerMockito.doNothing().when(DataRetrieverClient.class, "retrieveDataBlocks", Mockito.anyString(),
					Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyInt(), Mockito.anyLong(),
					Mockito.anyInt(), Mockito.anyLong(), Mockito.anyInt());
				Mockito.when(fileSystemConfig.getServerPort()).thenReturn(9898);
			Mockito.when(fileSystemConfig.getMaxQueryAttempts()).thenReturn(10);
			Mockito.when(fileSystemConfig.getQueryRetryInterval()).thenReturn(1000L);
			Mockito.when(fileSystemConfig.getFileStreamBufferSize()).thenReturn(1024);

			DataRetrieverClient.retrieveDataBlocks(nodeIp, fileZKNode, dataFilePaths, outputDirName, dataSize);

			assertTrue(true);
		} catch (Exception e) {
			e.printStackTrace();
			assertTrue(false);
		}
	}

}
