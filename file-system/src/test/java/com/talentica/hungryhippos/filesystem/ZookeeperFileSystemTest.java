package com.talentica.hungryhippos.filesystem;

import static org.junit.Assert.*;

import org.junit.Test;

public class ZookeeperFileSystemTest {

	@Test
	public void testCreateFilesAsZnode() {
		ZookeeperFileSystem.createFilesAsZnode("/abcd/input.txt");
	}

	/**
	 * Positive test scenario, already a Znode is created in the Zookeeper.
	 */
	@Test
	public void testGetDataInsideZnode() {
		FileMetaData fileMetaData = ZookeeperFileSystem
				.getDataInsideZnode("/home/sudarshans/RD/HungryHippos/utility/sampledata.txt");
		assertNotNull(fileMetaData);
		assertNotNull(fileMetaData.getFileName());
		assertTrue(fileMetaData.getSize() >= 0);
		assertNotNull(fileMetaData.getType());
	}
}
