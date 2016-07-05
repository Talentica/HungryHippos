package com.talentica.hungryhippos.filesystem;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;

import javax.xml.bind.JAXBException;

import org.junit.Test;

import com.talentica.hungryhippos.config.client.CoordinationServers;
import com.talentica.hungryhippos.config.client.ObjectFactory;

public class ZookeeperFileSystemTest {

	private ZookeeperFileSystem system;

	@Test
	public void testCreateFilesAsZnode() throws FileNotFoundException, JAXBException {
		CoordinationServers coordinationServers = new ObjectFactory().createCoordinationServers();
		system = new ZookeeperFileSystem();
		system.createFilesAsZnode("/abcd/input.txt");
	}

	/**
	 * Positive test scenario, already a Znode is created in the Zookeeper.
	 * @throws JAXBException 
	 * @throws FileNotFoundException 
	 */
	@Test
	public void testGetDataInsideZnode() throws FileNotFoundException, JAXBException {
		CoordinationServers coordinationServers = new ObjectFactory().createCoordinationServers();
		system = new ZookeeperFileSystem();
		FileMetaData fileMetaData = system
				.getDataInsideZnode("/home/sudarshans/RD/HungryHippos/utility/sampledata.txt");
		assertNotNull(fileMetaData);
		assertNotNull(fileMetaData.getFileName());
		assertTrue(fileMetaData.getSize() >= 0);
		assertNotNull(fileMetaData.getType());
	}
}
