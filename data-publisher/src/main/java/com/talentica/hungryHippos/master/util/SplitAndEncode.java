package com.talentica.hungryHippos.master.util;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.FileUtils;

public class SplitAndEncode {

  private static String SCRIPT_FOR_FILE_SPLIT = "hh-split-file.sh";
  
  public static void main(String[] args){
    String shardingFolderPath = args[0];
    String inputFileName = args[1];
    String outputFolder = args[2];
    String scriptPath = FileUtils.getUserDirectoryPath() + File.separator + "split-script" + File.separator + SCRIPT_FOR_FILE_SPLIT;
    String commonCommandArgs = scriptPath + " " + inputFileName
        + " " + 40;
    ExecutorService executorService = Executors.newFixedThreadPool(8);
    for(int i = 1; i <= 40; i++){
      executorService.execute(new EncodeFileThread(inputFileName,shardingFolderPath, outputFolder + i, commonCommandArgs, i));
    }
    executorService.shutdown();
  }
}
