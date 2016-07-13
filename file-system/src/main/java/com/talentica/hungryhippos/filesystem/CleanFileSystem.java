package com.talentica.hungryhippos.filesystem;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	private static final String USER_DIR = System.getProperty("user.dir");

	/**
	 * return all the regular files in the folder provided as an argument as
	 * well as regular files in its subfolder.
	 * 
	 * i.e; /{user.dir}/HungryHippos/data/data.txt -> regular file will get be
	 * added to the list. /{user.dir}/HungryHippos/data -> is a directory and
	 * will not get added.
	 * 
	 * 
	 * @param path
	 * @return
	 */

	public static List<Path> getAllRgularFilesPath(String path) {
		List<Path> filesPresentInRootFolder = new ArrayList<>();
		try {
			Files.walk(Paths.get(path)).forEach(filePath -> {
				if (Files.isRegularFile(filePath)) {
					filesPresentInRootFolder.add(filePath);
				}
			});
		} catch (IOException e) {
			logger.error(e.getMessage());

		}
		return filesPresentInRootFolder;
	}

	/**
	 * return all the directory in the folder provided as an argument as well as
	 * in its subfolder.
	 * 
	 * i.e; /{user.dir}/HungryHippos/data/data.txt -> regular file will get be
	 * added to the list. /{user.dir}/HungryHippos/data -> is a directory and
	 * will not get added.
	 * 
	 * 
	 * @param path
	 * @return
	 */

	public static List<Path> getAllDirectoryPath(String path) {
		List<Path> filesPresentInRootFolder = new ArrayList<>();
		try {
			Files.walk(Paths.get(path)).forEach(filePath -> {
				if (Files.isDirectory(filePath)) {
					filesPresentInRootFolder.add(filePath);
				}
			});
		} catch (IOException e) {
			logger.error(e.getMessage());

		}
		return filesPresentInRootFolder;
	}

	/**
	 * Delete a file. fails when the folder has children
	 * 
	 * @param path
	 */

	public static void deleteFile(Path path) {
		if (path.toString().equals(USER_DIR + ROOT_DIR)) {
			return;
		}
		try {
			Files.delete(path);
		} catch (IOException e) {
			logger.error(e.getMessage());
			throw new RuntimeException(e.getMessage());
		}
	}

	/**
	 * Deletes all the items inside a folder including that folder.
	 * 
	 * @param path
	 */

	public static void deleteAllFilesInsideAFolder(Path path) {
		List<Path> regularFilesInFolder = getAllRgularFilesPath(path.toString());
		List<Path> directoryInsideFolder = getAllDirectoryPath(path.toString());

		regularFilesInFolder.stream().forEach(CleanFileSystem::deleteFile);
		// reversing the list so first subfolders of a folder is deleted first.
		directoryInsideFolder.stream().sorted().collect(Collectors.toCollection(ArrayDeque::new)).descendingIterator()
				.forEachRemaining(CleanFileSystem::deleteFile);

	}

	/**
	 * deletes the files which are not part of ZK.
	 * 
	 * @param path
	 */
	public static void DeleteFilesWhichAreNotPartOFZK(String path) {
		if (HungryHipposFileSystem.checkZnodeExists(path)) {
			return;
		}
		deleteAllFilesInsideAFolder(Paths.get(path));
	}

}
