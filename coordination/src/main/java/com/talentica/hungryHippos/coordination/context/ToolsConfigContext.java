package com.talentica.hungryHippos.coordination.context;

import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.domain.ZKNodeFile;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.tools.ToolsConfig;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.IOException;

/**
 * This class is for reading the Tools configuration
 * Created by rajkishoreh on 18/8/16.
 */
public class ToolsConfigContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(ToolsConfigContext.class);

    private static ToolsConfig toolsConfig;

    private static ToolsConfig getToolsConfig() {
        if (toolsConfig == null) {
            try {
                ZKNodeFile configFile = (ZKNodeFile) NodesManagerContext.getNodesManagerInstance()
                        .getConfigFileFromZNode(CoordinationApplicationContext.TOOLS_CONFIGURATION);
                toolsConfig = JaxbUtil.unmarshal((String) configFile.getObj(), ToolsConfig.class);
            } catch (ClassNotFoundException | KeeperException | InterruptedException | IOException e) {
                LOGGER.info("Please upload the job-runner configuration file on zookeeper");
            } catch (JAXBException e1) {
                LOGGER.info("Unable to unmarshal the job-runner xml configuration.");
            }
        }
        return toolsConfig;
    }

    public static int getServerPort() {
        return getToolsConfig().getServerPort();
    }

    public static long getQueryRetryInterval() {
        return getToolsConfig().getQueryRetryInterval();
    }


    public static int getMaxQueryAttempts() {
        return getToolsConfig().getMaxQueryAttempts();
    }

    public static int getMaxClientRequests() {
        return getToolsConfig().getMaxClientRequests();
    }
}
