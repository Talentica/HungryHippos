package com.talentica.hungryhippos.filesystem.main;

import java.io.File;

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
	 */
	public static void main(String[] args) {

		validateArgs(args);
		CleanFileSystem.DeleteFilesWhichAreNotPartOFZK(System.getProperty("user.dir") + File.separatorChar + ROOT_DIR,
				args[0]);
	}

	private static void validateArgs(String[] args) {

		if (args.length < 0) {
			throw new IllegalArgumentException("Need client-config.xml location details.");
		}
	}

}
