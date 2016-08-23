package com.talentica.hungryhippos.filesystem.main;

import java.io.File;
import java.io.FileNotFoundException;

import javax.xml.bind.JAXBException;

import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryhippos.filesystem.CleanFileSystem;

/**
 * 
 * @author sudarshans
 *
 */
public class CleanFileSystemMain {

	private static final String ROOT_DIR = "HungryHipposFs";

	/**
	 * 
	 * @param args
	 * @throws JAXBException 
	 * @throws FileNotFoundException 
	 */
	public static void main(String[] args) throws FileNotFoundException, JAXBException {

		validateArgs(args);
		NodesManagerContext.getNodesManagerInstance(args[0]);
		CleanFileSystem.DeleteFilesWhichAreNotPartOFZK(System.getProperty("user.dir") + File.separatorChar + ROOT_DIR);
	}

	private static void validateArgs(String[] args) {

		if (args.length < 0) {
			throw new IllegalArgumentException("Need client-config.xml location details.");
		}
	}

}
