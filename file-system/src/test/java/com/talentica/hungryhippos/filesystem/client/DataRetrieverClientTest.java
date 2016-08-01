// package com.talentica.hungryhippos.filesystem.client;
//
// import com.talentica.hungryHippos.coordination.NodesManager;
// import
// com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
// import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
// import com.talentica.hungryHippos.coordination.property.Property;
// import com.talentica.hungryHippos.coordination.property.ZkProperty;
// import com.talentica.hungryhippos.config.coordination.ClusterConfig;
// import com.talentica.hungryhippos.config.coordination.CoordinationConfig;
// import com.talentica.hungryhippos.config.coordination.Node;
// import com.talentica.hungryhippos.config.zookeeper.ZookeeperConfig;
// import com.talentica.hungryhippos.config.zookeeper.ZookeeperDefaultSetting;
// import com.talentica.hungryhippos.filesystem.FileSystemConstants;
// import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
// import com.talentica.hungryhippos.filesystem.util.FileSystemUtils;
// import org.junit.Before;
// import org.junit.Test;
// import org.junit.runner.RunWith;
/// *import org.mockito.Mockito;
// import org.powermock.api.mockito.PowerMockito;
// import org.powermock.core.classloader.annotations.PrepareForTest;
// import
// org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
// import org.powermock.modules.junit4.PowerMockRunner;*/
//
// import java.io.File;
// import java.util.ArrayList;
// import java.util.Arrays;
// import java.util.List;
//
// import static org.junit.Assert.assertTrue;
//
/// **
// * This is a Test class for DataRetrieverClient Created by rajkishoreh on
// * 11/7/16.
// *//*
// * @RunWith(PowerMockRunner.class)
// *
// * @PrepareForTest({ NodesManagerContext.class,
// * CoordinationApplicationContext.class, DataRetrieverClient.class,
// * FileSystemContext.class, FileSystemUtils.class })
// *
// * @SuppressStaticInitializationFor({
// * "com.talentica.hungryHippos.coordination.domain.NodesManagerContext" })
// */
// public class DataRetrieverClientTest {
//
// private NodesManager nodesManager;
// private ZookeeperConfig zookeeperConfig;
// private ZookeeperDefaultSetting zookeeperDefaultSetting;
// private ClusterConfig clusterConfig;
// private CoordinationConfig coordinationConfig;
// private Property<ZkProperty> zkProperty;
//
// @Before
// public void setUp() {
// /*
// * PowerMockito.mockStatic(NodesManagerContext.class);
// * PowerMockito.mockStatic(CoordinationApplicationContext.class);
// * PowerMockito.mockStatic(FileSystemContext.class);
// * PowerMockito.spy(DataRetrieverClient.class);
// * PowerMockito.spy(FileSystemUtils.class);
// *
// * nodesManager = Mockito.mock(NodesManager.class); zookeeperConfig =
// * Mockito.mock(ZookeeperConfig.class); zookeeperDefaultSetting =
// * Mockito.mock(ZookeeperDefaultSetting.class); clusterConfig =
// * Mockito.mock(ClusterConfig.class); coordinationConfig =
// * Mockito.mock(CoordinationConfig.class); zkProperty =
// * Mockito.mock(Property.class);
// */
// }
//
// @Test
// public void testGetHungryHippoData() {
// try {
// String relativeFilePath = "input";
// String outputDirName = System.getProperty("user.home") + "/data";
// int dimension = 2;
// String fsRootNode = "/rootnode/filesystem";
// List<String> dataFileNodes = Arrays.asList(new String[] { "0", "1", "2", "3"
// });
// int nodeId = 0;
// Node node = new Node();
// node.setIdentifier(nodeId);
// node.setIp("localhost");
// List<Node> nodeList = new ArrayList<>();
// nodeList.add(node);
//
// String fileNodeZKPath = fsRootNode + FileSystemConstants.ZK_PATH_SEPARATOR +
// relativeFilePath
// + FileSystemConstants.ZK_PATH_SEPARATOR + FileSystemConstants.DFS_NODE;
// String nodeIdZKPath = fileNodeZKPath + FileSystemConstants.ZK_PATH_SEPARATOR
// + nodeId;
// List<String> nodeIds = new ArrayList<>();
// nodeIds.add(nodeId + "");
// /*
// * PowerMockito.doNothing().when(DataRetrieverClient.class,
// * "retrieveDataBlocks", Mockito.anyString(), Mockito.anyString(),
// * Mockito.anyString(), Mockito.anyLong());
// * PowerMockito.doNothing().when(FileSystemUtils.class,
// * "createDirectory", Mockito.anyString());
// * PowerMockito.doNothing().when(FileSystemUtils.class,
// * "combineFiles", Mockito.anyCollection(),Mockito.anyString());
// * PowerMockito.doNothing().when(FileSystemUtils.class,
// * "deleteFiles", Mockito.anyCollection());
// * PowerMockito.when(NodesManagerContext.getNodesManagerInstance()).
// * thenReturn(nodesManager);
// * PowerMockito.when(CoordinationApplicationContext.
// * getCoordinationConfig()).thenReturn(coordinationConfig);
// * Mockito.when(coordinationConfig.getClusterConfig()).thenReturn(
// * clusterConfig);
// * Mockito.when(clusterConfig.getNode()).thenReturn(nodeList);
// * PowerMockito.when(NodesManagerContext.getZookeeperConfiguration()
// * ).thenReturn(zookeeperConfig);
// * Mockito.when(zookeeperConfig.getZookeeperDefaultSetting()).
// * thenReturn(zookeeperDefaultSetting);
// *
// *
// * for (String dataFileNode : dataFileNodes) {
// * Mockito.when(nodesManager.getObjectFromZKNode(nodeIdZKPath +
// * FileSystemConstants.ZK_PATH_SEPARATOR + dataFileNode))
// * .thenReturn("100"); }
// * Mockito.when(nodesManager.getChildren(fileNodeZKPath)).thenReturn
// * (nodeIds);
// * Mockito.when(nodesManager.getChildren(nodeIdZKPath)).thenReturn(
// * dataFileNodes);
// */
// DataRetrieverClient.getHungryHippoData(relativeFilePath, outputDirName,
// dimension);
//
// assertTrue(true);
// } catch (Exception e) {
// e.printStackTrace();
// assertTrue(false);
// }
//
// }
//
// @Test
// public void testRequestDataBlocks() {
// try {
// String relativeFilePath = "input";
// String outputDirName = System.getProperty("user.home") + "/data";
// String nodeIp = "localhost";
// long dataSize = 1000L;
//
// /*
// * PowerMockito.doNothing().when(DataRetrieverClient.class,
// * "retrieveDataBlocks", Mockito.anyString(), Mockito.anyString(),
// * Mockito.anyString(), Mockito.anyInt(), Mockito.anyLong(),
// * Mockito.anyInt(), Mockito.anyLong(), Mockito.anyInt());
// * PowerMockito.when(FileSystemContext.getServerPort()).thenReturn(
// * 9898);
// * PowerMockito.when(FileSystemContext.getMaxQueryAttempts()).
// * thenReturn(10);
// * PowerMockito.when(FileSystemContext.getQueryRetryInterval()).
// * thenReturn(1000L);
// * PowerMockito.when(FileSystemContext.getFileStreamBufferSize()).
// * thenReturn(1024);
// */
// DataRetrieverClient.retrieveDataBlocks(nodeIp, relativeFilePath,
// outputDirName, dataSize);
//
// assertTrue(true);
// } catch (Exception e) {
// e.printStackTrace();
// assertTrue(false);
// }
// }
//
// }
