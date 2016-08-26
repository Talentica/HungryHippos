package com.talentica.hungryhippos.filesystem.util;

import com.talentica.hungryHippos.coordination.ZkUtils;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This is a Test class for FileSystemUtils.
 * Created by rajkishoreh on 11/7/16.
 */
@Ignore
public class FileSystemUtilsTest {

    public String fileSystemRootNode;

    @Before
    public void setup() throws FileNotFoundException, JAXBException {
        NodesManagerContext.
                getNodesManagerInstance("/home/rajkishoreh/hungry/HungryHippos/configuration-schema/src/main/resources/schema/client-config.xml");
        fileSystemRootNode = CoordinationConfigUtil.getZkCoordinationConfigCache().
                getZookeeperDefaultConfig().getFilesystemPath();
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
    public void testValidatePathIsAFile() {
        String filePath = "/dir1/dir2/file1.txt";
        FileSystemUtils.validatePath(filePath,true);
        Assert.assertTrue(true);
    }

    @Test
    public void testValidatePathIsADirectory() {
        String filePath = "/dir1/dir2/";
        FileSystemUtils.validatePath(filePath,false);
        Assert.assertTrue(true);
    }

    @Test
    public void testValidatePath() {
        String filePath = "/dir1/dir2/file1.txt";
        FileSystemUtils.validatePath(filePath,true);
        ZkUtils.createFileNode(fileSystemRootNode+filePath);
        FileSystemUtils.validatePath(filePath,true);
        String filePath1 = "/dir1/dir2/";
        FileSystemUtils.validatePath(filePath1,false);
        String filePath2 = "/dir1/dir2/file2.txt";
        FileSystemUtils.validatePath(filePath2,true);

        try {
            String filePath3 = "/dir1/dir2/file1.txt/dir3/file2.txt";
            FileSystemUtils.validatePath(filePath3, true);
            Assert.assertTrue(false);
        }catch (RuntimeException e){
            e.printStackTrace();
            Assert.assertTrue(true);
        }


    }
    @Test
    public void testValidatePathInValid() {
        try {
            String filePath = "../dir1/dir2/text";
            FileSystemUtils.validatePath(filePath, true);
            Assert.assertTrue(false);
        } catch (RuntimeException e) {
            e.printStackTrace();
            Assert.assertTrue(true);
        }

        try {
            String filePath = "/dir1/dir2/../text";
            FileSystemUtils.validatePath(filePath, true);
            Assert.assertTrue(false);
        } catch (RuntimeException e) {
            e.printStackTrace();
            Assert.assertTrue(true);
        }

        try {
            String filePath = "/dir1/dir2/./text";
            FileSystemUtils.validatePath(filePath, true);
            Assert.assertTrue(false);
        } catch (RuntimeException e) {
            e.printStackTrace();
            Assert.assertTrue(true);
        }

        try {
            String filePath = "./dir1/dir2/text";
            FileSystemUtils.validatePath(filePath, true);
            Assert.assertTrue(false);
        } catch (RuntimeException e) {
            e.printStackTrace();
            Assert.assertTrue(true);
        }

        try {
            String filePath = "/dir1/dir2/text/.";
            FileSystemUtils.validatePath(filePath, true);
            Assert.assertTrue(false);
        } catch (RuntimeException e) {
            e.printStackTrace();
            Assert.assertTrue(true);
        }

        try {
            String filePath = "/dir1/dir2/.text";
            FileSystemUtils.validatePath(filePath, true);
            Assert.assertTrue(false);
        } catch (RuntimeException e) {
            e.printStackTrace();
            Assert.assertTrue(true);
        }
    }
}
