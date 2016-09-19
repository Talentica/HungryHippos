package com.talentica.hungryhippos.filesystem;

import java.io.FileNotFoundException;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used for cleaning files which has no reference in the
 * zookeeper
 * 
 * @author sudarshans
 *
 */
public class CleanFileSystem {

	private static final Logger logger = LoggerFactory.getLogger(CleanFileSystem.class);

	private static void deleteFile(String loc) {
		NodeFileSystem.deleteFile(loc);
	}

	private static List<String> getAllFilesInaFolder(String loc) {
		return NodeFileSystem.getAllRgularFilesPath(loc);
	}

	/**
	 * deletes the files which are not part of ZK.
	 * 
	 * @param path
	 * @throws JAXBException
	 * @throws FileNotFoundException
	 */
	public static void DeleteFilesWhichAreNotPartOFZK(String path) throws FileNotFoundException, JAXBException {
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
