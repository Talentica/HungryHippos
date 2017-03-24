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
package com.talentica.hungryhippos.filesystem;

import java.io.FileNotFoundException;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used for cleaning files which has no reference in the zookeeper
 * 
 * @author sudarshans
 *
 */
public class CleanFileSystem {

  private static final Logger logger = LoggerFactory.getLogger(CleanFileSystem.class);
  private NodeFileSystem nodeFileSystem = null;

  public CleanFileSystem(NodeFileSystem nodeFileSystem) {
    this.nodeFileSystem = nodeFileSystem;
  }

  private void deleteFile(String loc) {
    nodeFileSystem.deleteFile(loc);
  }

  private List<String> getAllFilesInaFolder(String loc) {
    return nodeFileSystem.getAllRgularFilesPath(loc);
  }

  /**
   * deletes the files which are not part of ZK.
   * 
   * @param path
   * @throws JAXBException
   * @throws FileNotFoundException
   */
  public void deleteFilesWhichAreNotPartOFZK(String path)
      throws FileNotFoundException, JAXBException {
    HungryHipposFileSystem hhfs = HungryHipposFileSystem.getInstance();
    List<String> filesLoc = getAllFilesInaFolder(path);
    for (String fileLoc : filesLoc) {
      if (hhfs.checkZnodeExists(path)) {
        continue;
      } else {
        logger.info("deleting file " + fileLoc + " as its location is not present in zookeeper.");
        deleteFile(fileLoc);
      }
    }
  }

}
