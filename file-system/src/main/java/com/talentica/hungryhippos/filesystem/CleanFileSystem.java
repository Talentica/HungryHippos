package com.talentica.hungryhippos.filesystem;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryhippos.filesystem.ZookeeperFileSystem;

/**
 * This class is used for cleaning files which have no reference in the
 * zookeeper
 * 
 * @author sudarshans
 *
 */
public class CleanFileSystem {

	private static final Logger logger = LoggerFactory.getLogger(CleanFileSystem.class);
	private static final String ROOT_DIR = "HungryHipposFs";

	/**
	 * List all the file path in the HHFileSystem
	 * 
	 * @param path
	 * @return
	 */
	private List<Path> getAllFilesPath(String path) {
		List<Path> filesPresentInRootFolder = new ArrayList<>();
		try {
			Files.walk(Paths.get(path)).forEach(filePath -> {
				if (Files.isRegularFile(filePath)) {
					filesPresentInRootFolder.add(filePath);
				}
			});
		} catch (IOException e) {
			logger.error(e.getMessage());
			;
		}
		return filesPresentInRootFolder;
	}

	/**
	 * Delete a file. fails when the folder has children
	 * 
	 * @param path
	 */
	private void deleteFile(Path path) {
		try {
			Files.delete(path);
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}

	/**
	 * Deletes all the items inside a folder including that folder.
	 */
	private void deleteAllFilesInsideAFolder(Path path) {
		if (!Files.isDirectory(path)) {
			deleteFile(path);
		} else {
			List<Path> filesPresentInFolder = getAllFilesPath(path.toString());
			if (filesPresentInFolder.isEmpty()) {
				if (path.equals(System.getProperty("user.home") + File.separatorChar + ROOT_DIR)) {
					logger.info("can't delete the root folder of the node");
					return;
				}
				deleteFile(path);
			}
			for (Path path2 : filesPresentInFolder) {
				deleteAllFilesInsideAFolder(path2);
			}
		}
	}

	/**
	 * deletes the files which are not part of ZK.
	 * 
	 * @param path
	 */
	public void DeleteFilesWhichAreNotPartOFZK(String path) {
		if (ZookeeperFileSystem.checkZnodeExists(path)) {
			return;
		}
		deleteAllFilesInsideAFolder(Paths.get(path));
	}

}
