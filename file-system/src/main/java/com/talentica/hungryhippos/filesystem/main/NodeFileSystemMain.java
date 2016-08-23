package com.talentica.hungryhippos.filesystem.main;

import com.talentica.hungryhippos.filesystem.NodeFileSystem;

public class NodeFileSystemMain {

	public static void main(String[] args) {
		switch (args[0]) {
		case "create":
		case "touch":
			NodeFileSystem.createDirAndFile(args[1]);
			break;
		case "delete":
			NodeFileSystem.deleteFile(args[1]);
			break;
		case "deleteall":
			NodeFileSystem.deleteAllFilesInsideAFolder(args[1]);
			break;
		case "find":
			if (!(args.length <= 3)) {
				throw new IllegalArgumentException(
						"Arguments should be 3, find \"Directory to be searched\" \"pattern to search\" ");
			}
			NodeFileSystem.findFilesWithPattern(args);
			break;
		default:
			throw new UnsupportedOperationException();
		}

	}

}
