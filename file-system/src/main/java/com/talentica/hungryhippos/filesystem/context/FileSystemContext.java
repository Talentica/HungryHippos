package com.talentica.hungryhippos.filesystem.context;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.domain.ZKNodeFile;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.filesystem.FileSystemConfig;
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
		ZKNodeFile serverConfigFile = new ZKNodeFile(CoordinationApplicationContext.FILE_SYSTEM, fileSystemConfigFile);
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
	private static void getFileSystemConfig() {
		try {
			if (fileSystemConfig == null) {
				NodesManager manager = NodesManagerContext.getNodesManagerInstance();
				ZKNodeFile fileSystemConfigurationFile = (ZKNodeFile) manager
						.getConfigFileFromZNode(CoordinationApplicationContext.FILE_SYSTEM);
				fileSystemConfig = JaxbUtil.unmarshal((String) fileSystemConfigurationFile.getObj(),
						FileSystemConfig.class);
			}
		} catch (Exception e) {
			throw new RuntimeException(e.toString());
		}
	}

	public static String getRootDirectory() {
		getFileSystemConfig();
		return fileSystemConfig.getRootDirectory();
	}

	public static String getDataFilePrefix() {
		getFileSystemConfig();
		return fileSystemConfig.getDataFilePrefix();
	}

	public static boolean isAppendToDataFile() {
		getFileSystemConfig();
		return fileSystemConfig.isAppendToDataFile();
	}

	public static int getServerPort() {
		getFileSystemConfig();
		return fileSystemConfig.getServerPort();
	}

	public static long getQueryRetryInterval() {
		getFileSystemConfig();
		return fileSystemConfig.getQueryRetryInterval();
	}

	public static int getMaxQueryAttempts() {
		getFileSystemConfig();
		return fileSystemConfig.getMaxQueryAttempts();
	}

	public static int getFileStreamBufferSize() {
		getFileSystemConfig();
		return fileSystemConfig.getFileStreamBufferSize();
	}

	public static int getMaxClientRequests() {
		getFileSystemConfig();
		return fileSystemConfig.getMaxClientRequests();
	}

}
