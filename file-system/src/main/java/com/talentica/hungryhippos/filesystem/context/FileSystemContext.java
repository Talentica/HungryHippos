package com.talentica.hungryhippos.filesystem.context;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.domain.ZKNodeFile;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.filesystem.FileSystemConfig;
import com.talentica.hungryhippos.filesystem.FileSystemConstants;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * This class is for handling the filesystem configuration Created by
 * rajkishoreh on 4/7/16.
 */
public class FileSystemContext {

	private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemContext.class);

	private static FileSystemConfig fileSystemConfig;

	/**
	 * This method uploads the filesystem configuration file in zookeeper
	 * 
	 * @param fileSystemConfigFile
	 * @throws IOException
	 * @throws JAXBException
	 * @throws InterruptedException
	 */
	public static void uploadFileSystemConfig(String fileSystemConfigFile)
			throws IOException, JAXBException, InterruptedException {
		LOGGER.info("Updating filesystem configuration on zookeeper");
		ZKNodeFile serverConfigFile = new ZKNodeFile(FileSystemConstants.CONFIGURATION_FILE, fileSystemConfigFile);
		CountDownLatch countDownLatch = new CountDownLatch(1);
		NodesManagerContext.getNodesManagerInstance().saveConfigFileToZNode(serverConfigFile, countDownLatch);
		countDownLatch.await();
	}

	/**
	 * This method gets the filesystem configuration object from zookeeper
	 * filesystem configuration file
	 * 
	 * @return
	 * @throws IOException
	 * @throws JAXBException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 * @throws KeeperException
	 */

	private static void getFileSystemConfig()
			throws IOException, JAXBException, InterruptedException, ClassNotFoundException, KeeperException {

		if (fileSystemConfig == null) {
			NodesManager manager = NodesManagerContext.getNodesManagerInstance();
			ZKNodeFile fileSystemConfigurationFile = (ZKNodeFile) manager
					.getConfigFileFromZNode(FileSystemConstants.CONFIGURATION_FILE);
			fileSystemConfig = JaxbUtil.unmarshal((String) fileSystemConfigurationFile.getObj(),
					FileSystemConfig.class);
		}

	}

	public static String getRootDirectory()
			throws InterruptedException, ClassNotFoundException, JAXBException, KeeperException, IOException {
		getFileSystemConfig();
		return fileSystemConfig.getRootDirectory();
	}

	public static String getDataFilePrefix()
			throws InterruptedException, ClassNotFoundException, JAXBException, KeeperException, IOException {
		getFileSystemConfig();
		return fileSystemConfig.getDataFilePrefix();
	}

	public static int getServerPort()
			throws InterruptedException, ClassNotFoundException, JAXBException, KeeperException, IOException {
		getFileSystemConfig();
		return fileSystemConfig.getServerPort();
	}

	public static long getQueryRetryInterval()
			throws InterruptedException, ClassNotFoundException, JAXBException, KeeperException, IOException {
		getFileSystemConfig();
		return fileSystemConfig.getQueryRetryInterval();
	}

	public static int getMaxQueryAttempts()
			throws InterruptedException, ClassNotFoundException, JAXBException, KeeperException, IOException {
		getFileSystemConfig();
		return fileSystemConfig.getMaxQueryAttempts();
	}

	public static int getFileStreamBufferSize()
			throws InterruptedException, ClassNotFoundException, JAXBException, KeeperException, IOException {
		getFileSystemConfig();
		return fileSystemConfig.getFileStreamBufferSize();
	}

	public static int getMaxClientRequests()
			throws InterruptedException, ClassNotFoundException, JAXBException, KeeperException, IOException {
		getFileSystemConfig();
		return fileSystemConfig.getMaxClientRequests();
	}
}