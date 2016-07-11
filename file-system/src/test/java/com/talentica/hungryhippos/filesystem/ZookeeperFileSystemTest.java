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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ NodesManagerContext.class, CoordinationApplicationContext.class })
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
		zkProperty = Mockito.mock(Property.class);
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
