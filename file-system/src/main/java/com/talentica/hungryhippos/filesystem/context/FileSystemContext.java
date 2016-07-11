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
 * This class is for handling the filesystem configuration
 * Created by rajkishoreh on 4/7/16.
 */
public class FileSystemContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemContext.class);

    public static FileSystemConfig fileSystemConfig;

    /**
     * This method uploads the filesystem configuration file in zookeeper
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
     * This method gets the filesystem configuration object from zookeeper filesystem configuration file
     * @return
     * @throws IOException
     * @throws JAXBException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     * @throws KeeperException
     */
    public static FileSystemConfig getFileSystemConfig() throws IOException, JAXBException, InterruptedException, ClassNotFoundException, KeeperException {
        if(fileSystemConfig==null){
            NodesManager manager = NodesManagerContext.getNodesManagerInstance();
            ZKNodeFile fileSystemConfigurationFile = (ZKNodeFile) manager
                    .getConfigFileFromZNode(FileSystemConstants.CONFIGURATION_FILE);
            fileSystemConfig = JaxbUtil.unmarshal((String) fileSystemConfigurationFile.getObj(),
                    FileSystemConfig.class);
        }

        return fileSystemConfig;
    }
}
