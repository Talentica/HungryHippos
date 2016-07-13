package com.talentica.hungryhippos.filesystem.util;

import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This is a Test class for FileSystemUtils
 * Created by rajkishoreh on 11/7/16.
 */
public class FileSystemUtilsTest {

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
}
