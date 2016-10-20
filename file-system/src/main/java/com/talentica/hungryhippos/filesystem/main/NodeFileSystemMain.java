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
