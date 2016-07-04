package com.talentica.hungryhippos.filesystem;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.talentica.hungryhippos.config.client.CoordinationServers;
import com.talentica.hungryhippos.config.client.ObjectFactory;

public class ZookeeperFileSystemTest {

	private ZookeeperFileSystem system;

	@Test
	public void testCreateFilesAsZnode() {
		CoordinationServers coordinationServers = new ObjectFactory().createCoordinationServers();
		system = new ZookeeperFileSystem(coordinationServers);
		system.createFilesAsZnode("/abcd/input.txt");
	}

	/**
	 * Positive test scenario, already a Znode is created in the Zookeeper.
	 */
	@Test
	public void testGetDataInsideZnode() {
		CoordinationServers coordinationServers = new ObjectFactory().createCoordinationServers();
		system = new ZookeeperFileSystem(coordinationServers);
		FileMetaData fileMetaData = system
				.getDataInsideZnode("/home/sudarshans/RD/HungryHippos/utility/sampledata.txt");
		assertNotNull(fileMetaData);
		assertNotNull(fileMetaData.getFileName());
		assertTrue(fileMetaData.getSize() >= 0);
		assertNotNull(fileMetaData.getType());
	}
}
