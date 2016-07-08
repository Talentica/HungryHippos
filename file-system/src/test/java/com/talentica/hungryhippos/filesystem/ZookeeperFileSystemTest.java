package com.talentica.hungryhippos.filesystem;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.property.Property;
import com.talentica.hungryHippos.coordination.property.ZkProperty;
import com.talentica.hungryhippos.config.client.CoordinationServers;
import com.talentica.hungryhippos.config.client.ObjectFactory;
import com.talentica.hungryhippos.filesystem.client.DataRetrieverClient;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import com.talentica.hungryhippos.filesystem.property.FileSystemProperty;
import com.talentica.hungryhippos.filesystem.util.FileSystemUtils;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({NodesManagerContext.class,
        CoordinationApplicationContext.class,
        DataRetrieverClient.class,
        FileSystemContext.class,
        FileSystemUtils.class})
@SuppressStaticInitializationFor({"com.talentica.hungryHippos.coordination.domain.NodesManagerContext"})
public class ZookeeperFileSystemTest {

    private ZookeeperFileSystem system;

    private NodesManager nodesManager;
    private Property<FileSystemProperty> fileSystemProperty;
    private Property<ZkProperty> zkProperty;

    @Before
    public void setUp() {
        PowerMockito.mockStatic(NodesManagerContext.class);
        PowerMockito.mockStatic(CoordinationApplicationContext.class);
        PowerMockito.mockStatic(FileSystemContext.class);
        PowerMockito.spy(DataRetrieverClient.class);
        PowerMockito.spy(FileSystemUtils.class);

        nodesManager = Mockito.mock(NodesManager.class);
        fileSystemProperty = Mockito.mock(Property.class);
        zkProperty = Mockito.mock(Property.class);
    }

    @Test
    public void testCreateFilesAsZnode() throws FileNotFoundException, JAXBException {
        CoordinationServers coordinationServers = new ObjectFactory().createCoordinationServers();
        system = new ZookeeperFileSystem();
        system.createFilesAsZnode("/abcd/input.txt");
    }

    /**
     * Positive test scenario, already a Znode is created in the Zookeeper.
     *
     * @throws JAXBException
     * @throws FileNotFoundException
     */
    @Test
    public void testGetDataInsideZnode() throws FileNotFoundException, JAXBException {
        CoordinationServers coordinationServers = new ObjectFactory().createCoordinationServers();
        system = new ZookeeperFileSystem();
        FileMetaData fileMetaData = system
                .getDataInsideZnode("/home/sudarshans/RD/HungryHippos/utility/sampledata.txt");
        assertNotNull(fileMetaData);
        assertNotNull(fileMetaData.getFileName());
        assertTrue(fileMetaData.getSize() >= 0);
        assertNotNull(fileMetaData.getType());
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
            List<String> dataFileNodes = Arrays.asList(new String[]{"0", "1", "2", "3"});
            String nodeIp = "localhost";
            String fileNodeZKPath = fileSystemRootNodeZKPath + File.separator + fileZKNode;
            String nodeIpZKPath = fileNodeZKPath + File.separator + nodeIp;
            List<String> nodeIps = new ArrayList<>();
            nodeIps.add(nodeIp);


            PowerMockito.doNothing().when(DataRetrieverClient.class, "retrieveDataBlocks", Mockito.anyString(), Mockito.anyString(),
                    Mockito.anyString(), Mockito.anyString(), Mockito.anyLong());
            PowerMockito.doNothing().when(FileSystemUtils.class, "createDirectory", Mockito.anyString());
            PowerMockito.when(NodesManagerContext.getNodesManagerInstance()).thenReturn(nodesManager);
            PowerMockito.when(CoordinationApplicationContext.getZkProperty()).thenReturn(zkProperty);

            for (String dataFileNode : dataFileNodes) {
                Mockito.when(nodesManager.getObjectFromZKNode(nodeIpZKPath + File.separator + dataFileNode)).thenReturn("100");
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

            PowerMockito.doNothing().when(DataRetrieverClient.class, "retrieveDataBlocks", Mockito.anyString(), Mockito.anyString(),
                    Mockito.anyString(), Mockito.anyString(), Mockito.anyInt(), Mockito.anyLong(), Mockito.anyInt(), Mockito.anyLong(), Mockito.anyInt());
            PowerMockito.when(FileSystemContext.getProperty()).thenReturn(fileSystemProperty);
            Mockito.when(fileSystemProperty.getValueByKey(FileSystemConstants.SERVER_PORT)).thenReturn("9898");
            Mockito.when(fileSystemProperty.getValueByKey(FileSystemConstants.MAX_QUERY_ATTEMPTS)).thenReturn("10");
            Mockito.when(fileSystemProperty.getValueByKey(FileSystemConstants.QUERY_RETRY_INTERVAL)).thenReturn("1000");
            Mockito.when(fileSystemProperty.getValueByKey(FileSystemConstants.FILE_STREAM_BUFFER_SIZE)).thenReturn("1024");

            DataRetrieverClient.retrieveDataBlocks(nodeIp, fileZKNode, dataFilePaths, outputDirName, dataSize);

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
            Mockito.doNothing().when(nodesManager).createPersistentNode(Mockito.anyString(), new CountDownLatch(1), Mockito.eq(Mockito.any()));
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
