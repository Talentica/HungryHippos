/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
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
package com.talentica.hungryhippos.filesystem.helper;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBException;

import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryhippos.config.cluster.Node;
import com.talentica.hungryhippos.filesystem.HungryHipposFileSystem;
import com.talentica.hungryhippos.filesystem.main.HungryHipposCommandLauncher;

import jline.Completor;

/**
 * {@code TabCompletor} used for showing tab key recommendations.
 * 
 * @author sudarshans
 *
 */
public class TabCompletor implements Completor {

  /**
   * method used for completing the options when client presses tab key.
   */
  @SuppressWarnings("unchecked")
  public int complete(String buffer, int cursor, List candidates) {

    buffer = buffer.substring(0, cursor);
    String token = "";
    if (!buffer.endsWith(" ")) {
      String[] tokens = buffer.split(" ");
      if (tokens.length != 0) {
        token = tokens[tokens.length - 1];
      }
    }

    if (token.startsWith("/")) {
      return completeZNode(buffer, token, candidates);
    }
    return completeCommand(buffer, token, candidates);
  }


  private int completeCommand(String buffer, String token, List<String> candidates) {
    for (String cmd : HungryHipposCommandLauncher.getCommands()) {
      if (cmd.startsWith(token)) {
        candidates.add(cmd);
      }
    }
    return buffer.lastIndexOf(" ") + 1;
  }


  private List<String> fileSystemConstants = new ArrayList<>();

  public TabCompletor() {
    fileSystemConstants.add(FileSystemConstants.DATA_READY);
    fileSystemConstants.add(FileSystemConstants.DATA_SERVER_AVAILABLE);
    fileSystemConstants.add(FileSystemConstants.DATA_SERVER_BUSY);
    fileSystemConstants.add(FileSystemConstants.DATA_TRANSFER_COMPLETED);
    fileSystemConstants.add(FileSystemConstants.DFS_NODE);
    fileSystemConstants.add(FileSystemConstants.DOWNLOAD_FILE_PREFIX);
    fileSystemConstants.add(FileSystemConstants.IS_A_FILE);
    fileSystemConstants.add(FileSystemConstants.PUBLISH_FAILED);
    fileSystemConstants.add(FileSystemConstants.SHARDED);
  }

  private boolean validate(String token) {
    boolean flag = false;
    for (String constants : fileSystemConstants) {
      if (token.contains(constants)) {
        flag = true;
        break;
      }
    }

    return flag;
  }

  private int completeZNode(String buffer, String token, List<String> candidates) {
    if (!validate(token)) {
      String path = token;
      int idx = path.lastIndexOf("/") + 1;
      String prefix = path.substring(idx);
      try {
        // Only the root path can end in a /, so strip it off every other prefix


        String dir = idx == 1 ? "" : path.substring(0, idx - 1);

        List<String> children;
        children = HungryHipposFileSystem.getInstance().getChildZnodes(dir);
        boolean isFileConstant = false;
        if (children != null) {
          for (String child : children) {
            if (child.startsWith(prefix)) {
              for (String constants : fileSystemConstants) {
                if (child.equals(constants)) {
                  isFileConstant = true;
                  break;
                }

              }
          
              if (!isFileConstant) {
                candidates.add(child);
              }
            }
          }
        }
      } catch (FileNotFoundException | JAXBException e) {

        e.printStackTrace();
      }

    }
    return candidates.size() == 0 ? buffer.length() : buffer.lastIndexOf("/") + 1;



  }



}
