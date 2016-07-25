package com.talentica.hungryhippos.filesystem.util;

import java.io.*;
import java.util.List;

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
        if(!directory.isDirectory()){
            throw new RuntimeException(dirName+" is exists and is not a directory");
        }
    }

    /**
     * Combines multiple files into a single file
     * @param fileNames
     * @param destFile
     * @throws IOException
     */
    public static void combineFiles(List<String> fileNames, String destFile) throws IOException {

       for (String fileName:fileNames){
            File file = new File(fileName);
            if(!(file.exists()&&file.isFile())){
                throw new RuntimeException(fileName+" is not a file");
            }
        };
        FileOutputStream fos = new FileOutputStream(destFile);
        BufferedOutputStream bos = new BufferedOutputStream(fos);
        int len;
        FileInputStream fis;
        BufferedInputStream bis;
        byte[] buffer= new byte[2048];
        for (String fileName:fileNames){
            fis = new FileInputStream(fileName);
            bis = new BufferedInputStream(fis);
            while((len=bis.read(buffer))>-1){
                bos.write(buffer,0,len);;
            }
            bos.flush();
            bis.close();
        }
        bos.close();
    }

    /**
     * Deletes a list of files
     * @param fileNames
     */
    public static void deleteFiles(List<String> fileNames){
        for(String fileName:fileNames){
            new File(fileName).delete();
        }
    }



}
