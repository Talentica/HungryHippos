/**
 * 
 */
package com.talentica.hungryHippos.coordination.context;

import java.io.IOException;

import javax.xml.bind.JAXBException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.domain.ZKNodeFile;
import com.talentica.hungryHippos.coordination.property.Property;
import com.talentica.hungryHippos.coordination.property.ZkProperty;
import com.talentica.hungryHippos.coordination.utility.CoordinationProperty;
import com.talentica.hungryHippos.coordination.utility.ServerProperty;
import com.talentica.hungryhippos.config.client.CoordinationServers;

/**
 * @author pooshans
 *
 */
public class CoordinationApplicationContext {

	private static final Logger LOGGER = LoggerFactory.getLogger(CoordinationApplicationContext.class);

	private static Property<CoordinationProperty> property;
	private static Property<ZkProperty> zkProperty;
	private static Property<ServerProperty> serverProperty;
	private static FieldTypeArrayDataDescription dataDescription;

	public static final String SERVER_CONF_FILE = "serverConfigFile.properties";

	public static final String CLUSTER_CONFIGURATION = "cluster-configuration";

	public static final String COMMON_CONF_FILE_STRING = "common-config.properties";

	public CoordinationApplicationContext(CoordinationServers coordinationServers) {

	}

	public static Property<CoordinationProperty> getProperty() {
		if (property == null) {
			property = new CoordinationProperty("coordination-config.properties");
		}
		return property;
	}

	public static Property<ZkProperty> getZkProperty() {
		if (zkProperty == null) {
			zkProperty = new ZkProperty("zookeeper.properties");
		}
		return zkProperty;
	}

	public static Property<ServerProperty> getServerProperty() {
		if (serverProperty == null) {
			serverProperty = new ServerProperty("server-config.properties");
		}
		return serverProperty;
	}

	public static final FieldTypeArrayDataDescription getConfiguredDataDescription() {
		if (property == null) {
			getProperty();
		}
		if (dataDescription == null) {
			dataDescription = FieldTypeArrayDataDescription.createDataDescription(
					((CoordinationProperty) property).getDataTypeConfiguration(),
					((CoordinationProperty) property).getMaximumSizeOfSingleDataBlock());
		}
		return dataDescription;
	}

	public static String[] getShardingDimensions() {
		if (property == null) {
			getProperty();
		}
		String keyOrderString = property.getValueByKey("sharding_dimensions").toString();
		return keyOrderString.split(",");
	}

	public static int[] getShardingIndexes() {
		if (property == null) {
			getProperty();
		}
		String keyOrderString = property.getValueByKey("sharding_dimensions").toString();
		String[] shardingKeys = keyOrderString.split(",");
		int[] shardingKeyIndexes = new int[shardingKeys.length];
		String keysNamingPrefix = property.getValueByKey("keys.prefix");
		int keysNamingPrefixLength = keysNamingPrefix.length();
		for (int i = 0; i < shardingKeys.length; i++) {
			shardingKeyIndexes[i] = Integer.parseInt(shardingKeys[i].substring(keysNamingPrefixLength)) - 1;
		}
		return shardingKeyIndexes;
	}

	public static int getShardingIndexSequence(int keyId) {
		int[] shardingIndexes = getShardingIndexes();
		int index = -1;
		for (int i = 0; i < shardingIndexes.length; i++) {
			if (shardingIndexes[i] == keyId) {
				index = i;
				break;
			}
		}
		return index;
	}

	public static String[] getKeyNamesFromIndexes(int[] keyIndexes) {
		String[] keyColumnNames = getColumnsConfiguration();
		String[] result = new String[keyIndexes.length];
		for (int i = 0; i < keyIndexes.length; i++) {
			result[i] = keyColumnNames[keyIndexes[i]];
		}
		return result;
	}

	public static String[] getColumnsConfiguration() {
		String[] keyColumnNames = property.getValueByKey("column.names").toString().split(",");
		return keyColumnNames;
	}

	public static final String[] getDataTypeConfiguration() {
		if (property == null) {
			getProperty();
		}
		return property.getValueByKey("column.datatype-size").toString().split(",");
	}

	public static final int getMaximumSizeOfSingleDataBlock() {
		if (property == null) {
			getProperty();
		}
		return Integer.parseInt(property.getValueByKey("maximum.size.of.single.block.data"));
	}

	public static void loadAllProperty() {
		getProperty();
		getZkProperty();
		getServerProperty();
	}

	public static void updateClusterConfiguration(String clusterConfigurationFile,
			CoordinationServers coordinationServers) throws IOException, JAXBException {
		LOGGER.info("Updating cluster configuration on zookeeper");
		ZKNodeFile serverConfigFile = new ZKNodeFile(CLUSTER_CONFIGURATION, clusterConfigurationFile);
		NodesManagerContext.getNodesManagerInstance().saveConfigFileToZNode(serverConfigFile, null);
	}
}
