package com.talentica.hungryhippos.filesystem;

import static org.junit.Assert.*;

import org.junit.Test;

public class CleanFileSystemTest {

	@Test
	public void testDeleteFilesWhichAreNotPartOFZK() {
		CleanFileSystem cfs = new CleanFileSystem();
		cfs.DeleteFilesWhichAreNotPartOFZK("");
	}

}
