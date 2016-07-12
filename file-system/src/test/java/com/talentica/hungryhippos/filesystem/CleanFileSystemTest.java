package com.talentica.hungryhippos.filesystem;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.junit.Test;

public class CleanFileSystemTest {

	@Test
	public void testGetAllFilesPath() {

		List<Path> regularFiles = CleanFileSystem.getAllRgularFilesPath("/home/sudarshans/hungryhippos");
		assertNotNull(regularFiles);
		assertNotEquals(regularFiles.size(), -1);
	}

	@Test
	public void testGetAllDirectoryPath() {

		List<Path> regularFiles = CleanFileSystem.getAllDirectoryPath("/home/sudarshans/hungryhippos");
		assertNotNull(regularFiles);
		assertNotEquals(regularFiles.size(), -1);
	}

	/**
	 * Test should fail as the hungryhippos folder is not empty.
	 */
	@Test
	public void testDeleteFile() {

		try {
			CleanFileSystem.deleteFile(Paths.get("/home/sudarshans/hungryhippos"));
			assertFalse(true);
		} catch (RuntimeException e) {
			assertTrue(true);
		}
	}

	/**
	 * Test will pass even when argument folder is not empty. it will remove all
	 * the subfolder of the argument folder. Note:- It shouldn't
	 * deleteHungryHipposFileSystem.
	 */
	@Test
	public void testDeleteAllFile() {

		try {
			CleanFileSystem.deleteAllFilesInsideAFolder(Paths.get("/home/sudarshans/hungryhippos"));
			assertFalse(true);
		} catch (RuntimeException e) {
			assertTrue(true);
		}
	}

	@Test
	public void testDeleteFilesWhichAreNotPartOFZK() {

		CleanFileSystem.DeleteFilesWhichAreNotPartOFZK("");
	}

}
