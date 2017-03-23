/*******************************************************************************
 * Copyright [2017] [Talentica Software Pvt. Ltd.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
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
