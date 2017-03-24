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
package com.talentica.hungryhippos.filesystem.main;

import java.io.FileNotFoundException;

import javax.xml.bind.JAXBException;

import com.talentica.hungryhippos.filesystem.NodeFileSystem;

public class NodeFileSystemMain {

  public static void main(String[] args) throws FileNotFoundException, JAXBException {
    
    NodeFileSystem nodeFileSystem = new NodeFileSystem(args[0]);
    switch (args[1]) {
      case "delete":
        nodeFileSystem.deleteFile(args[2]);
        break;
      case "deleteall":
        nodeFileSystem.deleteAllFilesInsideAFolder(args[2]);
        break;
      case "find":
        if (!(args.length <= 4)) {
          throw new IllegalArgumentException(
              "Arguments should be 3, find \"Directory to be searched\" \"pattern to search\" ");
        }
        nodeFileSystem.findFilesWithPattern(args);
        break;
      default:
        throw new UnsupportedOperationException();
    }

  }

}
