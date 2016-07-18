package com.talentica.hungryhippos.filesystem.main;

import com.talentica.hungryhippos.filesystem.NodeFileSystems;

public class NodeFileSystemMain {

  public static void main(String[] args) {
    switch (args[0]) {
      case "FileCreation":
        NodeFileSystems.createFile(args[1]);
        break;
      case "DirCreation":
        NodeFileSystems.createDir(args[1]);
        break;
      case "Delete":
        NodeFileSystems.deleteFile(args[1]);
        break;
      case "DeleteAll":
        NodeFileSystems.deleteAllFilesInsideAFolder(args[1]);
        break;
      default:
        throw new UnsupportedOperationException();
    }

  }

}
