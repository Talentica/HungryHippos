package com.talentica.hungryhippos.filesystem.util;

import java.io.File;

/**
 * Created by rajkishoreh on 8/7/16.
 */
public class FileSystemUtils {

    /**
     * This method returns the equivalent dimension Operand
     *
     * @param dimension
     * @return
     */
    public static int getDimensionOperand(int dimension) {
        if (dimension < 0) {
            throw new RuntimeException("Invalid Dimension : " + dimension);
        }
        //Determines the dataFileNodes from which the data has to be retrieved.
        int dimensionOperand = 1;
        if (dimension > 0) {
            dimensionOperand = 1 << (dimension - 1);
        } else {
            //if dimension specified is zero , then all the dataFileNodes are selected.
            dimensionOperand = 0;
        }
        return dimensionOperand;
    }

    /**
     * This method creates a directory if it doesn't exist
     *
     * @param dirName
     */
    public static void createDirectory(String dirName) {
        File directory = new File(dirName);
        if (!directory.exists()) {
            directory.mkdirs();
        }
    }


}
